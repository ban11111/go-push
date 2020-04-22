package logic

import (
	"encoding/json"
	"github.com/ban11111/go-push/common"
)

type PushJob struct {
	pushType int // 推送类型
	roomId string // 房间ID
	items []json.RawMessage	 // 要推送的消息数组
}

type GateConnMgr struct {
	gateConns []*GateConn	// 到所有gateway的连接数组
	pendingChan []chan byte  // gateway的并发请求控制
	dispatchChan chan*PushJob // 待分发的推送
}

var (
	G_gateConnMgr *GateConnMgr
)

// 推送给一个gateway
func (gateConnMgr *GateConnMgr) doPush(gatewayIdx int, pushJob *PushJob, itemsJson []byte) {
	if pushJob.pushType == common.PushTypeAll {
		gateConnMgr.gateConns[gatewayIdx].PushAll(itemsJson)
	} else if pushJob.pushType == common.PushTypeRoom {
		gateConnMgr.gateConns[gatewayIdx].PushRoom(pushJob.roomId, itemsJson)
	} else if pushJob.pushType == common.PushTypeRoomOne {
		gateConnMgr.gateConns[gatewayIdx].PushRoomOne(pushJob.roomId, itemsJson)
	}

	// 释放名额
	<- gateConnMgr.pendingChan[gatewayIdx]
}

// 消息分发协程
func (gateConnMgr* GateConnMgr) dispatchWorkerMain(dispatchWorkerIdx int) {
	var (
		pushJob *PushJob
		gatewayIdx int
		itemsJson []byte
		err error
	)
	for {
		select {
		case pushJob = <- gateConnMgr.dispatchChan:
			// 序列化
			if itemsJson, err = json.Marshal(pushJob.items); err != nil {
				continue
			}
			// 分发到所有gateway
			for gatewayIdx = 0; gatewayIdx < len(gateConnMgr.gateConns); gatewayIdx++ {
				select {
				case gateConnMgr.pendingChan[gatewayIdx] <- 1:	// 并发控制
					go gateConnMgr.doPush(gatewayIdx, pushJob, itemsJson)
				default:	// 并发已满, 直接丢弃
				}
			}
		}
	}
}

func InitGateConnMgr() (err error) {
	var (
		gatewayIdx int
		dispatchWorkerIdx int
		gatewayConfig GatewayConfig
		gateConnMgr *GateConnMgr
	)

	gateConnMgr = &GateConnMgr{
		gateConns: make([]*GateConn, len(G_config.GatewayList)),
		pendingChan: make([]chan byte, len(G_config.GatewayList)),
		dispatchChan: make(chan*PushJob, G_config.GatewayDispatchChannelSize),
	}

	for gatewayIdx, gatewayConfig = range G_config.GatewayList {
		if gateConnMgr.gateConns[gatewayIdx], err = InitGateConn(&gatewayConfig); err != nil {
			return
		}
		gateConnMgr.pendingChan[gatewayIdx] = make(chan byte, G_config.GatewayMaxPendingCount)
	}

	for dispatchWorkerIdx = 0; dispatchWorkerIdx < G_config.GatewayDispatchWorkerCount; dispatchWorkerIdx++ {
		go gateConnMgr.dispatchWorkerMain(dispatchWorkerIdx)
	}

	G_gateConnMgr = gateConnMgr
	return
}

func (gateConnMgr *GateConnMgr) PushAll(items []json.RawMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeAll,
		items: items,
	}

	select {
	case gateConnMgr.dispatchChan <- pushJob:
		DispatchTotal_INCR(int64(len(items)))
	default:
		DispatchFail_INCR(int64(len(items)))
		err = common.ERR_LOGIC_DISPATCH_CHANNEL_FULL
	}
	return
}

func (gateConnMgr *GateConnMgr) PushRoom(roomId string, items []json.RawMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeRoom,
		roomId: roomId,
		items: items,
	}

	select {
	case gateConnMgr.dispatchChan <- pushJob:
		DispatchTotal_INCR(int64(len(items)))
	default:
		DispatchFail_INCR(int64(len(items)))
		err = common.ERR_LOGIC_DISPATCH_CHANNEL_FULL
	}
	return
}

func (gateConnMgr *GateConnMgr) PushRoomOne(roomId string, items []json.RawMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeRoomOne,
		roomId: roomId,
		items: items,
	}

	select {
	case gateConnMgr.dispatchChan <- pushJob:
		DispatchTotal_INCR(int64(len(items)))
	default:
		DispatchFail_INCR(int64(len(items)))
		err = common.ERR_LOGIC_DISPATCH_CHANNEL_FULL
	}
	return
}