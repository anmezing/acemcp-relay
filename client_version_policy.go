package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	cloudClientPackageName      = "@anmezing/lce-cloud"
	minimumClientVersionSetting = "minimum_client_version"
)

var strictClientVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)

func migrateClientVersionPolicy() error {
	if configured := currentMinClientVersion(); configured != "" && !strictClientVersionPattern.MatchString(configured) {
		return fmt.Errorf("MIN_CLIENT_VERSION must use major.minor.patch semver, got %q", configured)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS relay_runtime_settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return err
	}

	var stored string
	err := db.QueryRow(`SELECT value FROM relay_runtime_settings WHERE key = $1`, minimumClientVersionSetting).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if stored != "" && !strictClientVersionPattern.MatchString(stored) {
		return fmt.Errorf("stored minimum client version is invalid: %q", stored)
	}
	setMinClientVersion(stored)
	return nil
}

func saveMinimumClientVersion(version string) error {
	version = strings.TrimSpace(version)
	if version != "" && !strictClientVersionPattern.MatchString(version) {
		return errors.New("minimum_version must use major.minor.patch semver")
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 空字符串也必须持久化。若删除记录，重启后会重新落回环境变量的启动默认值，
	// 管理员在控制台执行的“关闭门禁”便会失效。
	_, err = tx.Exec(`
		INSERT INTO relay_runtime_settings (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
	`, minimumClientVersionSetting, version)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	setMinClientVersion(version)
	return nil
}

func clientVersionPolicyResponse() gin.H {
	minimumVersion := currentMinClientVersion()
	return gin.H{
		"package":                       cloudClientPackageName,
		"minimum_version":               minimumVersion,
		"index_client_version_required": minimumVersion != "",
	}
}

func handleGetClientVersionPolicy(c *gin.Context) {
	if !isTrustedConsoleRequest(c) {
		c.JSON(http.StatusNotFound, gin.H{"error": "route not found"})
		return
	}
	c.JSON(http.StatusOK, clientVersionPolicyResponse())
}

func handleSaveClientVersionPolicy(c *gin.Context) {
	if !isTrustedConsoleRequest(c) {
		c.JSON(http.StatusNotFound, gin.H{"error": "route not found"})
		return
	}
	var input map[string]json.RawMessage
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "minimum_version is required"})
		return
	}
	raw, exists := input["minimum_version"]
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "minimum_version is required"})
		return
	}
	version := ""
	if strings.TrimSpace(string(raw)) != "null" {
		if err := json.Unmarshal(raw, &version); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "minimum_version must be a string or null"})
			return
		}
	}
	if err := saveMinimumClientVersion(version); err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "must use") {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, clientVersionPolicyResponse())
}
