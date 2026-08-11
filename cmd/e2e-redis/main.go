// Command e2e-redis 用 miniredis 起一个独立的内存 Redis 进程，供 E2E 环境使用：
// relay 启动强依赖 Redis，而 E2E 环境没有真实实例。
//
// 用法：e2e-redis -addr 127.0.0.1:6379
// 就绪后向 stdout 打印 "READY <addr>"，随后阻塞运行，收到 SIGINT/SIGTERM 退出。
//
// 注：miniredis 原本只作为测试依赖存在；本命令 import 后 go mod tidy 会把它
// 转为主模块依赖，属预期行为。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alicebob/miniredis/v2"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "listen address for the miniredis server")
	flag.Parse()

	srv := miniredis.NewMiniRedis()
	if err := srv.StartAddr(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "e2e-redis: failed to listen on %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer srv.Close()

	fmt.Printf("READY %s\n", srv.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Fprintf(os.Stderr, "e2e-redis: received %v, shutting down\n", sig)
}
