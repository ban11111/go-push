package gateway

import (
	"fmt"
	"os"
	"time"
)

func InitGateWayServer(cfg *Config) (err error) {
	// 统计
	if err = InitStats(); err != nil {
		goto ERR
	}

	// 初始化连接管理器
	if err = InitConnMgr(); err != nil {
		goto ERR
	}

	// 初始化websocket服务器
	if err = InitWSServer(); err != nil {
		goto ERR
	}

	// 初始化merger合并层
	if err = InitMerger(); err != nil {
		goto ERR
	}

	// 初始化service接口
	if err = InitService(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	os.Exit(0)

ERR:
	fmt.Fprintln(os.Stderr, err)
	os.Exit(-1)
}