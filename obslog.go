package main

// ── 结构化日志 helper ───────────────────────────────────────────────────────
//
// 不做全仓库日志改造：只提供一个 logfmt 一行式 helper，把请求生命周期的关键
// 事件（请求进入、上游 LCE 失败、426 拒绝、配额拒绝、panic recovery）换成可
// 机器解析的格式。request_id 直接复用请求日志表的 log_id（ContextKeyLogID），
// 并通过 context 贯穿到 LCE 调用的错误日志，响应头以 X-Request-Id 回传。

import (
	"context"
	"log"
	"strconv"
	"strings"
)

type requestIDCtxKey struct{}

func withRequestID(ctx context.Context, requestID string) context.Context {
	if requestID == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDCtxKey{}, requestID)
}

func requestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDCtxKey{}).(string)
	return id
}

// needsLogfmtQuoting 判断 value 是否需要引号包裹才能保持单 token 语义。
func needsLogfmtQuoting(value string) bool {
	if value == "" {
		return true
	}
	return strings.ContainsAny(value, " \t\r\n\"=")
}

// formatLogEvent 生成一行 logfmt：`evt=<event> k1=v1 k2=v2 ...`。
// pairs 是 key/value 交替序列；奇数尾巴会补一个空值，保证输出总是成对。
func formatLogEvent(event string, pairs ...string) string {
	var b strings.Builder
	b.WriteString("evt=")
	b.WriteString(event)
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		value := ""
		if i+1 < len(pairs) {
			value = pairs[i+1]
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		if needsLogfmtQuoting(value) {
			b.WriteString(strconv.Quote(value))
		} else {
			b.WriteString(value)
		}
	}
	return b.String()
}

func logEvent(event string, pairs ...string) {
	log.Println(formatLogEvent(event, pairs...))
}
