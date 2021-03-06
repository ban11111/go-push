package gateway

import "github.com/ban11111/go-push/common"

// 推送任务
type PushJob struct {
	pushType int    // 推送类型
	roomId   string // 房间ID
	// union {
	bizMsg *common.BizMessage // 未序列化的业务消息
	wsMsg  *common.WSMessage  //  已序列化的业务消息
	// }
}

// 连接管理器
type ConnMgr struct {
	buckets []*Bucket
	jobChan []chan *PushJob // 每个Bucket对应一个Job Queue

	dispatchChan chan *PushJob // 待分发消息队列
}

func NewConnMgr(cfg *Config) *ConnMgr {
	mgr := &ConnMgr{
		buckets:      make([]*Bucket, cfg.BucketCount),
		jobChan:      make([]chan *PushJob, cfg.BucketCount),
		dispatchChan: make(chan *PushJob, cfg.DispatchChannelSize),
	}
	for bucketIdx := range mgr.buckets {
		mgr.buckets[bucketIdx] = InitBucket(bucketIdx)                         // 初始化Bucket
		mgr.jobChan[bucketIdx] = make(chan *PushJob, cfg.BucketJobChannelSize) // Bucket的Job队列
	}
	return mgr
}

var (
	G_connMgr *ConnMgr
)

// 消息分发到Bucket
func (connMgr *ConnMgr) dispatchWorkerMain(dispatchWorkerIdx int, pushedOne chan bool) {
	var (
		bucketIdx int
		pushJob   *PushJob
		err       error
	)
	for {
		select {
		case pushJob = <-connMgr.dispatchChan:
			DispatchPending_DESC()

			// 序列化
			if pushJob.wsMsg, err = common.EncodeWSMessage(pushJob.bizMsg); err != nil {
				continue
			}
			// 分发给所有Bucket, 若Bucket拥塞则等待
			for bucketIdx, _ = range connMgr.buckets {
				PushJobPending_INCR()
				connMgr.jobChan[bucketIdx] <- pushJob
				if pushJob.pushType == common.PushTypeRoomOne {
					if one := <-pushedOne; one {
						break
					}
				}
			}
		}
	}
}

// Job负责消息广播给客户端
func (connMgr *ConnMgr) jobWorkerMain(jobWorkerIdx int, bucketIdx int, pushedOne chan bool) {
	var (
		bucket  = connMgr.buckets[bucketIdx]
		pushJob *PushJob
	)

	for {
		select {
		case pushJob = <-connMgr.jobChan[bucketIdx]: // 从Bucket的job queue取出一个任务
			PushJobPending_DESC()
			if pushJob.pushType == common.PushTypeAll {
				bucket.PushAll(pushJob.wsMsg)
			} else if pushJob.pushType == common.PushTypeRoom {
				bucket.PushRoom(pushJob.roomId, pushJob.wsMsg)
			} else if pushJob.pushType == common.PushTypeRoomOne {
				pushedOne <- bucket.PushRoomOne(pushJob.roomId, pushJob.wsMsg)
			}
		}
	}
}

func (connMgr *ConnMgr) RunAsync() {
	pushedOne := make(chan bool)
	for bucketIdx := range connMgr.buckets {
		// Bucket的Job worker
		for jobWorkerIdx := 0; jobWorkerIdx < G_config.BucketJobWorkerCount; jobWorkerIdx++ {
			go connMgr.jobWorkerMain(jobWorkerIdx, bucketIdx, pushedOne)
		}
	}
	// 初始化分发协程, 用于将消息扇出给各个Bucket
	for dispatchWorkerIdx := 0; dispatchWorkerIdx < G_config.DispatchWorkerCount; dispatchWorkerIdx++ {
		go connMgr.dispatchWorkerMain(dispatchWorkerIdx, pushedOne)
	}
}

/**
以下是API
*/

func InitConnMgr() (err error) {
	var (
		bucketIdx         int
		jobWorkerIdx      int
		dispatchWorkerIdx int
		connMgr           *ConnMgr
	)

	connMgr = NewConnMgr(G_config)

	pushedOne := make(chan bool)
	for bucketIdx, _ = range connMgr.buckets {
		// Bucket的Job worker
		for jobWorkerIdx = 0; jobWorkerIdx < G_config.BucketJobWorkerCount; jobWorkerIdx++ {
			go connMgr.jobWorkerMain(jobWorkerIdx, bucketIdx, pushedOne)
		}
	}
	// 初始化分发协程, 用于将消息扇出给各个Bucket
	for dispatchWorkerIdx = 0; dispatchWorkerIdx < G_config.DispatchWorkerCount; dispatchWorkerIdx++ {
		go connMgr.dispatchWorkerMain(dispatchWorkerIdx, pushedOne)
	}

	G_connMgr = connMgr
	return
}

func (connMgr *ConnMgr) GetBucket(wsConnection *WSConnection) (bucket *Bucket) {
	bucket = connMgr.buckets[wsConnection.connId%uint64(len(connMgr.buckets))]
	return
}

func (connMgr *ConnMgr) AddConn(wsConnection *WSConnection) {
	var (
		bucket *Bucket
	)

	bucket = connMgr.GetBucket(wsConnection)
	bucket.AddConn(wsConnection)

	OnlineConnections_INCR()
}

func (connMgr *ConnMgr) DelConn(wsConnection *WSConnection) {
	var (
		bucket *Bucket
	)

	bucket = connMgr.GetBucket(wsConnection)
	bucket.DelConn(wsConnection)

	OnlineConnections_DESC()
}

func (connMgr *ConnMgr) JoinRoom(roomId string, wsConn *WSConnection) (err error) {
	var (
		bucket *Bucket
	)

	bucket = connMgr.GetBucket(wsConn)
	err = bucket.JoinRoom(roomId, wsConn)
	return
}

func (connMgr *ConnMgr) LeaveRoom(roomId string, wsConn *WSConnection) (err error) {
	var (
		bucket *Bucket
	)

	bucket = connMgr.GetBucket(wsConn)
	err = bucket.LeaveRoom(roomId, wsConn)
	return
}

// 向所有在线用户发送消息
func (connMgr *ConnMgr) PushAll(bizMsg *common.BizMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeAll,
		bizMsg:   bizMsg,
	}

	select {
	case connMgr.dispatchChan <- pushJob:
		DispatchPending_INCR()
	default:
		err = common.ERR_DISPATCH_CHANNEL_FULL
		DispatchFail_INCR()
	}
	return
}

// 向指定房间发送消息
func (connMgr *ConnMgr) PushRoom(roomId string, bizMsg *common.BizMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeRoom,
		bizMsg:   bizMsg,
		roomId:   roomId,
	}

	select {
	case connMgr.dispatchChan <- pushJob:
		DispatchPending_INCR()
	default:
		err = common.ERR_DISPATCH_CHANNEL_FULL
		DispatchFail_INCR()
	}
	return
}

// 向指定房间发送消息给一个
func (connMgr *ConnMgr) PushRoomOne(roomId string, bizMsg *common.BizMessage) (err error) {
	var (
		pushJob *PushJob
	)

	pushJob = &PushJob{
		pushType: common.PushTypeRoomOne,
		bizMsg:   bizMsg,
		roomId:   roomId,
	}

	select {
	case connMgr.dispatchChan <- pushJob:
		DispatchPending_INCR()
	default:
		err = common.ERR_DISPATCH_CHANNEL_FULL
		DispatchFail_INCR()
	}
	return
}
