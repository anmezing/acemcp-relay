package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// prepareIndexBatch 的错误分类是 handleRemoteIndex 状态码的唯一依据：
//
//	sql.ErrNoRows        -> 404 "index job not found"
//	其它错误             -> 409 + 错误原文
//
// 因此"哪些情况允许外传 sql.ErrNoRows"是一条必须守住的不变量。它内部有三处
// 查询都可能返回 ErrNoRows，但只有第一处（job 本身）表示任务不存在；另外两处
// 若把 ErrNoRows 原样外传，就会把"这个文件不在清单里"报成"任务丢了"，排查时
// 指向完全错误的方向。

const jobColumnCount = 20

// jobRow 构造 loadIndexJobFrom 期望的那一行（列顺序必须与其 SELECT 一致）。
func jobRow(status string, deletedCount int) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "workspace_id", "workspace_name", "branch", "revision", "mode", "phase", "status",
		"workspace_files", "total_files", "indexed_files", "failed_files", "total_chunks",
		"indexed_chunks", "chunk_count_fallback", "deleted_count", "error",
		"started_at", "heartbeat_at", "completed_at",
	}).AddRow(
		"job-1", "ws-1", "workspace", "main", "rev-1", "full", "indexing", status,
		10, 10, 0, 0, int64(0),
		int64(0), false, deletedCount, "",
		now, now, nil,
	)
}

// withMockTx 在一个 sqlmock 事务里运行 fn，并断言所有预期查询都被用到。
func withMockTx(t *testing.T, setup func(mock sqlmock.Sqlmock), fn func(tx *sql.Tx)) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()

	mock.ExpectBegin()
	setup(mock)

	tx, err := mockDB.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	fn(tx)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestPrepareIndexBatchReportsMissingJobAsErrNoRows(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnError(sql.ErrNoRows)
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", remoteIndexRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: "h1"}},
		})
		// 这是唯一允许外传 ErrNoRows 的情况：任务真的不存在（或不属于该用户）
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("job 不存在时应外传 sql.ErrNoRows，实际得到: %v", err)
		}
	})
}

func TestPrepareIndexBatchDoesNotDisguiseUnknownFileAsMissingJob(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(jobRow(indexJobStatusRunning, 0))
		// 该文件不在这个 job 的清单里
		mock.ExpectQuery("FROM index_job_files").
			WithArgs("job-1", "ghost.go").
			WillReturnError(sql.ErrNoRows)
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", remoteIndexRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "ghost.go", Content: "x", Hash: "h1"}},
		})
		if err == nil {
			t.Fatal("文件不在清单里时应当报错")
		}
		// 关键断言：不能是 ErrNoRows，否则调用方会回 404 index job not found
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("文件级 ErrNoRows 不应外传，会被误判成 job 不存在: %v", err)
		}
		if !strings.Contains(err.Error(), "not part of this job") {
			t.Errorf("错误消息应说明文件不属于该 job，实际: %v", err)
		}
		// 错误里要带上是哪个文件，否则客户端无从定位
		if !strings.Contains(err.Error(), "ghost.go") {
			t.Errorf("错误消息应包含出问题的文件名，实际: %v", err)
		}
	})
}

func TestPrepareIndexBatchDoesNotDisguiseVanishedJobAsMissingJob(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(jobRow(indexJobStatusRunning, 0))
		mock.ExpectQuery("FROM index_job_files").
			WithArgs("job-1", "a.go").
			WillReturnRows(sqlmock.NewRows([]string{"hash", "estimated_chunks", "needs_index", "indexed"}).
				AddRow("h1", 3, true, false))
		// job 在暂存过程中被并发删除
		mock.ExpectQuery("SELECT deletions_sent FROM index_jobs").
			WithArgs("job-1").
			WillReturnError(sql.ErrNoRows)
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", remoteIndexRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: "h1"}},
		})
		if err == nil {
			t.Fatal("job 中途消失时应当报错")
		}
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("deletions_sent 的 ErrNoRows 不应外传: %v", err)
		}
		if !strings.Contains(err.Error(), "disappeared") {
			t.Errorf("错误消息应说明 job 中途消失，实际: %v", err)
		}
	})
}

// 分类正确之外，正常路径也要能走通，否则上面三个用例可能只是"永远报错"。
func TestPrepareIndexBatchStagesFilesOnHappyPath(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(jobRow(indexJobStatusRunning, 0))
		mock.ExpectQuery("FROM index_job_files").
			WithArgs("job-1", "a.go").
			WillReturnRows(sqlmock.NewRows([]string{"hash", "estimated_chunks", "needs_index", "indexed"}).
				AddRow("h1", 7, true, false))
		mock.ExpectQuery("SELECT deletions_sent FROM index_jobs").
			WithArgs("job-1").
			WillReturnRows(sqlmock.NewRows([]string{"deletions_sent"}).AddRow(true))
	}, func(tx *sql.Tx) {
		job, staged, deleted, err := prepareIndexBatch(context.Background(), tx, "user-1", remoteIndexRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: "h1"}},
		})
		if err != nil {
			t.Fatalf("正常批次不应报错: %v", err)
		}
		if job.ID != "job-1" {
			t.Errorf("job.ID = %q, 期望 job-1", job.ID)
		}
		if len(staged) != 1 || staged[0].Path != "a.go" {
			t.Fatalf("应暂存 1 个文件 a.go, 实际: %+v", staged)
		}
		// 服务端记录的 hash 与预估分块数应覆盖客户端上报值
		if staged[0].EstimatedChunks != 7 {
			t.Errorf("EstimatedChunks = %d, 期望 7（取自服务端记录）", staged[0].EstimatedChunks)
		}
		if len(deleted) != 0 {
			t.Errorf("deletions_sent=true 时不应再下发删除列表, 实际: %v", deleted)
		}
	})
}

// 陈旧/已索引的文件走的是另一条分支，同样不能退化成 ErrNoRows。
func TestPrepareIndexBatchRejectsStaleFileWithoutErrNoRows(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(jobRow(indexJobStatusRunning, 0))
		mock.ExpectQuery("FROM index_job_files").
			WithArgs("job-1", "a.go").
			WillReturnRows(sqlmock.NewRows([]string{"hash", "estimated_chunks", "needs_index", "indexed"}).
				AddRow("server-hash", 3, true, false))
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", remoteIndexRequest{
			JobID: "job-1",
			// 客户端上报的 hash 与服务端记录不一致
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: "client-hash"}},
		})
		if err == nil {
			t.Fatal("hash 不匹配时应当报错")
		}
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("陈旧文件不应表现为 ErrNoRows: %v", err)
		}
		if !strings.Contains(err.Error(), "stale or already indexed") {
			t.Errorf("错误消息应说明文件陈旧, 实际: %v", err)
		}
	})
}
