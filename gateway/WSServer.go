package gateway

import (
	"crypto/tls"
	"net/http"
	"time"
	"net"
	"strconv"
	"github.com/gorilla/websocket"
	"sync/atomic"
)

// 	WebSocket服务端
type WSServer struct {
	server *http.Server
	listener net.Listener
	curConnId uint64
}

var (
	G_wsServer *WSServer

	wsUpgrader = websocket.Upgrader{
		// 允许所有CORS跨域请求
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func handleConnect(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		wsSocket *websocket.Conn
		connId uint64
		wsConn *WSConnection
	)

	// WebSocket握手
	if wsSocket, err = wsUpgrader.Upgrade(resp, req, nil); err != nil {
		return
	}

	// 连接唯一标识
	connId = atomic.AddUint64(&G_wsServer.curConnId, 1)

	// 初始化WebSocket的读写协程
	wsConn = InitWSConnection(connId, wsSocket)

	// 开始处理websocket消息
	wsConn.WSHandle()
}

func NewWsServer(cfg *Config) (s *WSServer, err error) {
	var mux *http.ServeMux
	// 路由
	mux = http.NewServeMux()
	mux.HandleFunc("/connect", handleConnect)

	s = &WSServer{
		server: &http.Server{
			ReadTimeout: time.Duration(cfg.WsReadTimeout) * time.Millisecond,
			WriteTimeout: time.Duration(cfg.WsWriteTimeout) * time.Millisecond,
			Handler: mux,
		},
		curConnId: uint64(time.Now().Unix()),
	}
	// 监听端口
	if s.listener, err = net.Listen("tcp", ":" + strconv.Itoa(cfg.WsPort)); err != nil {
		return
	}
	if cfg.ServerPem != "" && cfg.ServerKey != "" {
		cer, err := tls.X509KeyPair([]byte(cfg.ServerPem), []byte(cfg.ServerKey))
		if err != nil {
			return nil, err
		}
		s.server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cer}}
		s.listener = tls.NewListener(s.listener, &tls.Config{Certificates: []tls.Certificate{cer}})
	}

	return
}

func (s *WSServer) RunAsync()  {
	go s.server.Serve(s.listener)
}

func InitWSServer() (err error) {
	var (
		mux *http.ServeMux
		server *http.Server
		listener net.Listener
	)

	// 路由
	mux = http.NewServeMux()
	mux.HandleFunc("/connect", handleConnect)

	// HTTP服务
	server = &http.Server{
		ReadTimeout: time.Duration(G_config.WsReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.WsWriteTimeout) * time.Millisecond,
		Handler: mux,
	}

	// 监听端口
	if listener, err = net.Listen("tcp", ":" + strconv.Itoa(G_config.WsPort)); err != nil {
		return
	}

	// 赋值全局变量
	G_wsServer = &WSServer{
		server: server,
		curConnId: uint64(time.Now().Unix()),
	}

	// 拉起服务
	go server.Serve(listener)

	return
}