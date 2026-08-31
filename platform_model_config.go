package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var platformModelConfigSections = map[string]struct{}{
	"embeddings":     {},
	"rerank":         {},
	"promptEnhancer": {},
}

var (
	platformModelConfigBarrier sync.RWMutex
	platformModelConfigAdminMu sync.Mutex
)

func platformModelConfigReadBarrier() gin.HandlerFunc {
	return func(c *gin.Context) {
		platformModelConfigBarrier.RLock()
		defer platformModelConfigBarrier.RUnlock()
		c.Next()
	}
}

func requirePlatformConfigConsole(c *gin.Context) bool {
	if isTrustedConsoleRequest(c) {
		return true
	}
	c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "route not found"})
	return false
}

func callLCEPlatformConfig(ctx context.Context, method string, body interface{}) ([]byte, int, error) {
	if strings.TrimSpace(lcePlatformConfigToken) == "" {
		return nil, 0, fmt.Errorf("LCE platform config token is not configured")
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, lcePlatformConfigURL, reader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-LCE-Platform-Config-Token", lcePlatformConfigToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := lce.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxPlatformModelConfigBody)+1))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if len(data) > maxPlatformModelConfigBody {
		return nil, resp.StatusCode, fmt.Errorf("LCE platform config response too large")
	}
	return data, resp.StatusCode, nil
}

func handleGetPlatformModelConfig(c *gin.Context) {
	if !requirePlatformConfigConsole(c) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), platformModelConfigReadTimeout)
	defer cancel()
	data, status, err := callLCEPlatformConfig(ctx, http.MethodGet, nil)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "读取 LCE 模型配置失败"})
		return
	}
	c.Data(status, "application/json", data)
}

type clearedRelayIndexes struct {
	Jobs       int64 `json:"jobs"`
	Files      int64 `json:"files"`
	Workspaces int64 `json:"workspaces"`
	Leases     int64 `json:"leases"`
}

func clearAllRelayIndexState(ctx context.Context) (clearedRelayIndexes, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return clearedRelayIndexes{}, err
	}
	defer tx.Rollback()
	var cleared clearedRelayIndexes
	statements := []struct {
		query string
		count *int64
	}{
		{query: `DELETE FROM index_operation_leases`, count: &cleared.Leases},
		{query: `DELETE FROM index_jobs`, count: &cleared.Jobs},
		{query: `DELETE FROM indexed_files`, count: &cleared.Files},
		{query: `DELETE FROM index_workspaces`, count: &cleared.Workspaces},
	}
	for _, statement := range statements {
		result, execErr := tx.ExecContext(ctx, statement.query)
		if execErr != nil {
			return clearedRelayIndexes{}, execErr
		}
		*statement.count, _ = result.RowsAffected()
	}
	if err := tx.Commit(); err != nil {
		return clearedRelayIndexes{}, err
	}
	return cleared, nil
}

func lceConfigError(data []byte, fallback string) string {
	var response struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &response) == nil && strings.TrimSpace(response.Error) != "" {
		return response.Error
	}
	return fallback
}

func platformModelConfigTimedOut(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func acquirePlatformModelConfigWriteBarrier(ctx context.Context) error {
	if platformModelConfigBarrier.TryLock() {
		return nil
	}
	ticker := time.NewTicker(platformModelConfigLockPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if platformModelConfigBarrier.TryLock() {
				return nil
			}
		}
	}
}

func parsePlatformModelConfigPatch(section string, raw json.RawMessage) (map[string]interface{}, error) {
	section = strings.TrimSpace(section)
	if _, ok := platformModelConfigSections[section]; !ok {
		return nil, fmt.Errorf("section must be embeddings, rerank, or promptEnhancer")
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope == nil {
		return nil, fmt.Errorf("config must be a JSON object")
	}
	if len(envelope) != 1 {
		return nil, fmt.Errorf("config must contain exactly one model section")
	}
	payload, ok := envelope[section]
	if !ok {
		return nil, fmt.Errorf("config section must match section %q", section)
	}

	var sectionConfig map[string]interface{}
	if err := json.Unmarshal(payload, &sectionConfig); err != nil || sectionConfig == nil {
		return nil, fmt.Errorf("config.%s must be a JSON object", section)
	}
	return map[string]interface{}{section: sectionConfig}, nil
}

func handleSavePlatformModelConfig(c *gin.Context) {
	if !requirePlatformConfigConsole(c) {
		return
	}
	var request struct {
		Action                string          `json:"action"`
		Section               string          `json:"section"`
		Kind                  string          `json:"kind"`
		Provider              string          `json:"provider"`
		BaseURL               string          `json:"baseUrl"`
		APIKey                string          `json:"apiKey"`
		Config                json.RawMessage `json:"config"`
		ConfirmEmbeddingReset bool            `json:"confirmEmbeddingReset"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if request.Action == "models" {
		ctx, cancel := context.WithTimeout(c.Request.Context(), platformModelDiscoveryTimeout)
		defer cancel()
		data, status, err := callLCEPlatformConfig(ctx, http.MethodPost, map[string]interface{}{
			"action":   "models",
			"kind":     request.Kind,
			"provider": request.Provider,
			"baseUrl":  request.BaseURL,
			"apiKey":   request.APIKey,
		})
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": "获取供应商模型失败"})
			return
		}
		c.Data(status, "application/json", data)
		return
	}
	if len(request.Config) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "config is required"})
		return
	}

	config, err := parsePlatformModelConfigPatch(request.Section, request.Config)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if request.ConfirmEmbeddingReset && request.Section != "embeddings" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmEmbeddingReset is only valid for embeddings"})
		return
	}
	if !platformModelConfigAdminMu.TryLock() {
		c.JSON(http.StatusConflict, gin.H{
			"code":  "MODEL_CONFIG_SAVE_IN_PROGRESS",
			"error": "另一项模型配置正在验证或保存，请等待其结束后重试",
		})
		return
	}
	defer platformModelConfigAdminMu.Unlock()

	validationCtx, cancelValidation := context.WithTimeout(c.Request.Context(), platformModelConfigValidationTimeout)
	validatedData, validatedStatus, err := callLCEPlatformConfig(validationCtx, http.MethodPost, map[string]interface{}{
		"action": "validate",
		"config": config,
	})
	validationTimedOut := platformModelConfigTimedOut(validationCtx, err)
	cancelValidation()
	if err != nil {
		if validationTimedOut {
			c.JSON(http.StatusGatewayTimeout, gin.H{
				"code":  "MODEL_CONFIG_VALIDATION_TIMEOUT",
				"error": "模型连接验证超时；请检查供应商地址、网络、余额或服务状态后重试",
			})
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "LCE 模型连接验证失败"})
		return
	}
	if validatedStatus < 200 || validatedStatus >= 300 {
		status := validatedStatus
		if status < 400 || status > 599 {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": lceConfigError(validatedData, "模型配置验证失败")})
		return
	}
	var validated struct {
		EmbeddingChanged bool   `json:"embeddingChanged"`
		ValidationTicket string `json:"validationTicket"`
	}
	if err := json.Unmarshal(validatedData, &validated); err != nil || strings.TrimSpace(validated.ValidationTicket) == "" {
		c.JSON(http.StatusBadGateway, gin.H{"error": "LCE 返回了无效的验证响应"})
		return
	}
	if validated.EmbeddingChanged && !request.ConfirmEmbeddingReset {
		c.JSON(http.StatusConflict, gin.H{
			"error":                  "修改 Embedding 配置会清除现有向量索引，需确认后保存",
			"requiresEmbeddingReset": true,
		})
		return
	}

	clearedRelay := clearedRelayIndexes{}
	if validated.EmbeddingChanged {
		barrierCtx, cancelBarrier := context.WithTimeout(c.Request.Context(), platformModelConfigBarrierWait)
		err = acquirePlatformModelConfigWriteBarrier(barrierCtx)
		barrierTimedOut := errors.Is(barrierCtx.Err(), context.DeadlineExceeded)
		cancelBarrier()
		if err != nil {
			if barrierTimedOut {
				c.JSON(http.StatusConflict, gin.H{
					"code":  "EMBEDDING_SWITCH_BUSY",
					"error": "当前仍有索引或检索请求在执行，暂时不能切换 Embedding；请等待任务结束后重试",
				})
				return
			}
			return
		}
		defer platformModelConfigBarrier.Unlock()

		clearCtx, cancelClear := context.WithTimeout(c.Request.Context(), platformModelConfigRelayClearTimeout)
		clearedRelay, err = clearAllRelayIndexState(clearCtx)
		clearTimedOut := platformModelConfigTimedOut(clearCtx, err)
		cancelClear()
		if err != nil {
			if clearTimedOut {
				c.JSON(http.StatusGatewayTimeout, gin.H{
					"code":  "RELAY_INDEX_CLEAR_TIMEOUT",
					"error": "清理 Relay 旧索引状态超时，配置未切换；请稍后重试",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "清理旧索引失败，配置未切换"})
			return
		}
	}

	saveTimeout := platformModelConfigSimpleSaveTimeout
	if validated.EmbeddingChanged {
		saveTimeout = platformModelConfigResetSaveTimeout
	}
	saveCtx, cancelSave := context.WithTimeout(c.Request.Context(), saveTimeout)
	savedData, savedStatus, err := callLCEPlatformConfig(saveCtx, http.MethodPost, map[string]interface{}{
		"action":                "save",
		"config":                config,
		"confirmEmbeddingReset": request.ConfirmEmbeddingReset,
		"validationTicket":      validated.ValidationTicket,
	})
	saveTimedOut := platformModelConfigTimedOut(saveCtx, err)
	cancelSave()
	if err != nil {
		message := "LCE 未切换模型配置；请重试保存"
		if validated.EmbeddingChanged {
			message = "Relay 索引状态已清理，但 LCE 未切换配置；请重试保存"
		}
		if saveTimedOut {
			message = "保存 LCE 模型配置超时；请确认配置是否已生效，刷新后再决定是否重试"
			if validated.EmbeddingChanged {
				message = "Relay 索引状态已清理，但保存 LCE 配置超时；请刷新确认配置状态后再重试"
			}
			c.JSON(http.StatusGatewayTimeout, gin.H{
				"code":  "MODEL_CONFIG_SAVE_TIMEOUT",
				"error": message,
			})
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": message})
		return
	}
	if savedStatus < 200 || savedStatus >= 300 {
		message := lceConfigError(savedData, "保存模型配置失败")
		if validated.EmbeddingChanged {
			message = "Relay 索引状态已清理，但 LCE 未切换配置：" + message
		}
		c.JSON(savedStatus, gin.H{"error": message})
		return
	}
	var saved map[string]interface{}
	if json.Unmarshal(savedData, &saved) != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "LCE 返回了无效的保存响应"})
		return
	}
	if validated.EmbeddingChanged {
		saved["clearedRelay"] = clearedRelay
	}
	c.JSON(http.StatusOK, saved)
}
