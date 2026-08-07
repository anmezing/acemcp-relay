package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

// prepareIndexBatch 的错误分类决定 codebase_index upload 返回给 Agent 的原因：
// sql.ErrNoRows 只表示 "index job not found"，其它错误保留具体上下文。
//
// 因此"哪些情况允许外传 sql.ErrNoRows"是一条必须守住的不变量。它内部的
// 查询里只有第一处（job 本身）允许外传 ErrNoRows；批量文件查询"查不到某个
// 文件"表现为结果集里缺行，deletions_sent 查询的 ErrNoRows 表示 job 并发
// 消失——这两种都必须包装成带上下文的错误，否则会把"这个文件不在清单里"
// 报成"任务丢了"，排查时指向完全错误的方向。

// jobFileColumns 是批量文件查询的列。
func jobFileRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"path", "hash", "size", "estimated_chunks", "needs_index", "indexed"})
}

const singleByteXHash = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"

const jobColumnCount = 22

// jobRow 构造 loadIndexJobFrom 期望的那一行（列顺序必须与其 SELECT 一致）。
func jobRow(status string, deletedCount int) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "workspace_id", "workspace_name", "root_id", "branch", "revision", "mode", "phase", "status",
		"workspace_files", "total_files", "indexed_files", "failed_files", "total_chunks",
		"indexed_chunks", "chunk_count_fallback", "deleted_count", "error", "cloud_revision",
		"started_at", "heartbeat_at", "completed_at",
	}).AddRow(
		"job-1", "ws-1", "workspace", "", "main", "rev-1", "full", "indexing", status,
		10, 10, 0, 0, int64(0),
		int64(0), false, deletedCount, "", int64(0),
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
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: "h1"}},
		})
		// 这是唯一允许外传 ErrNoRows 的情况：任务真的不存在（或不属于该用户）
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("job 不存在时应外传 sql.ErrNoRows，实际得到: %v", err)
		}
	})
}

func TestClearUserIndexStateExecutesPortableSingleStatements(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("DELETE FROM index_jobs WHERE user_id").
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM indexed_files WHERE user_id").
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM index_workspaces WHERE user_id").
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}, func(tx *sql.Tx) {
		if err := clearUserIndexStateTx(context.Background(), tx, "user-1"); err != nil {
			t.Fatalf("clearUserIndexStateTx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})
}

func TestClearRootIndexStateMapsLegacyDefaultAndExecutesPortableSingleStatements(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("DELETE FROM index_jobs AS jobs").
			WithArgs("user-1", defaultLCEIndexRootID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM indexed_files AS files").
			WithArgs("user-1", defaultLCEIndexRootID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DELETE FROM index_workspaces").
			WithArgs("user-1", defaultLCEIndexRootID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}, func(tx *sql.Tx) {
		if err := clearRootIndexStateTx(context.Background(), tx, "user-1", ""); err != nil {
			t.Fatalf("clearRootIndexStateTx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})
}

func TestPrepareIndexBatchDoesNotDisguiseUnknownFileAsMissingJob(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(jobRow(indexJobStatusRunning, 0))
		// 该文件不在这个 job 的清单里：批量查询返回空结果集
		mock.ExpectQuery("FROM index_job_files").
			WithArgs("job-1", pq.Array([]string{"ghost.go"})).
			WillReturnRows(jobFileRows())
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "ghost.go", Content: "x", Hash: "h1"}},
		})
		if err == nil {
			t.Fatal("文件不在清单里时应当报错")
		}
		// 关键断言：不能是 ErrNoRows，否则调用方会误报 index job not found
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
			WithArgs("job-1", pq.Array([]string{"a.go"})).
			WillReturnRows(jobFileRows().AddRow("a.go", singleByteXHash, 1, 3, true, false))
		// job 在暂存过程中被并发删除
		mock.ExpectQuery("SELECT deletions_sent FROM index_jobs").
			WithArgs("job-1").
			WillReturnError(sql.ErrNoRows)
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: singleByteXHash}},
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
			WithArgs("job-1", pq.Array([]string{"a.go"})).
			WillReturnRows(jobFileRows().AddRow("a.go", singleByteXHash, 1, 7, true, false))
		mock.ExpectQuery("SELECT deletions_sent FROM index_jobs").
			WithArgs("job-1").
			WillReturnRows(sqlmock.NewRows([]string{"deletions_sent"}).AddRow(true))
	}, func(tx *sql.Tx) {
		job, staged, deleted, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
			JobID: "job-1",
			Files: []indexManifestFile{{Path: "a.go", Content: "x", Hash: singleByteXHash}},
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

func TestPrepareIndexBatchRejectsRootMismatch(t *testing.T) {
	withMockTx(t, func(mock sqlmock.Sqlmock) {
		row := jobRow(indexJobStatusRunning, 0)
		// jobRow uses the default root; the request below tries to switch roots mid-job.
		mock.ExpectQuery("FROM index_jobs").
			WithArgs("job-1", "user-1").
			WillReturnRows(row)
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
			JobID:  "job-1",
			RootID: "another-root",
			Files:  []indexManifestFile{{Path: "a.go", Content: "x", Hash: "h1"}},
		})
		if err == nil || !strings.Contains(err.Error(), "root_id does not match") {
			t.Fatalf("expected root mismatch error, got %v", err)
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
			WithArgs("job-1", pq.Array([]string{"a.go"})).
			WillReturnRows(jobFileRows().AddRow("a.go", "server-hash", 1, 3, true, false))
	}, func(tx *sql.Tx) {
		_, _, _, err := prepareIndexBatch(context.Background(), tx, "user-1", indexUploadRequest{
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
