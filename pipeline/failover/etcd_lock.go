package failover

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdLock 是写节点主备切换的互斥锁，逻辑与 consistency-checker 的 EtcdLock 一致，
// 只是把「版本切换」换成了「写节点主备切换」：
//
//	versionKey -> <chain_id>/<version>/writers/leader 或 <chain_id>/writers/leader
//	             由运维写入，值是被指定为主节点的 nodeID
//	leaderKey  -> <versionKey>/writeLock
//	             由主节点持有，带 lease，主节点退出或宕机后自动消失
//	nodeID     -> 本节点标识，对应 checker 里的 version 字段
//
// 互斥性来自 leaderKey 上的事务：CreateRevision == 0 才能写入，因此同一时刻只有
// 一个节点能持有 writeLock。旧主节点宕机后 lease 到期，key 被删除，新主节点才能拿到锁，
// 这个空窗期天然替代了原实现里的 gracePeriod。
//
// 光有 etcd 侧的互斥还不够：旧主与 etcd 断连时，它自己并不知道租约已在服务端过期。
// 因此本地记录「最近一次成功续约的发起时间 + TTL」作为到期时刻的保守估计（服务端续约
// 发生在请求发出之后，真实到期只会更晚），写入路径通过 Valid() 在到期前 safetyMargin
// 就停止写入。这样即使续约请求卡住、降级回调还没执行，也不会与新主节点双写。
type EtcdLock struct {
	leaderKey  string
	versionKey string
	nodeID     string
	client     *clientv3.Client
	ttl        int64
	onLost     func() // 续约失败时的回调

	lease clientv3.LeaseID // Acquire 成功后不再变化

	// base 提供单调时钟基准；expireAt 是相对 base 的到期偏移，避免墙上时钟跳变。
	base     time.Time
	expireAt atomic.Int64
	lost     atomic.Bool

	stop     chan struct{}
	stopOnce sync.Once
}

// NewLock 创建锁。
func NewLock(leaderKey, versionKey, nodeID string, client *clientv3.Client, ttl int64, onLost func()) *EtcdLock {
	return &EtcdLock{
		leaderKey:  leaderKey,
		versionKey: versionKey,
		nodeID:     nodeID,
		client:     client,
		ttl:        ttl,
		onLost:     onLost,
		base:       time.Now(),
		stop:       make(chan struct{}),
	}
}

// Acquire 尝试获取 writeLock，不阻塞等待旧锁释放。返回 false 表示本轮没拿到，
// 调用方会在下一个检查周期重试。
func (l *EtcdLock) Acquire() bool {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	start := time.Now()
	leaseResp, err := l.client.Grant(ctx, l.ttl)
	if err != nil {
		log.Printf("[Failover] Failed to create lease: %v", err)
		return false
	}
	l.lease = leaseResp.ID
	l.renewed(start, leaseResp.TTL)

	// 条件: leaderKey 不存在 AND versionKey 的值等于 nodeID
	txnResp, err := l.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(l.leaderKey), "=", 0),
		clientv3.Compare(clientv3.Value(l.versionKey), "=", l.nodeID),
	).
		Then(clientv3.OpPut(l.leaderKey, l.nodeID, clientv3.WithLease(l.lease))).
		Else(clientv3.OpGet(l.leaderKey), clientv3.OpGet(l.versionKey)).
		Commit()
	if err != nil {
		// 超时时事务可能已在服务端提交，Release 撤销租约会把 key 一并删掉。
		log.Printf("[Failover] Failed to commit transaction: %v", err)
		l.Release()
		return false
	}

	if !txnResp.Succeeded {
		holder, assigned := "<none>", "<none>"
		if kvs := txnResp.Responses[0].GetResponseRange().Kvs; len(kvs) > 0 {
			holder = string(kvs[0].Value)
		}
		if kvs := txnResp.Responses[1].GetResponseRange().Kvs; len(kvs) > 0 {
			assigned = string(kvs[0].Value)
		}
		log.Printf("[Failover] Cannot acquire %s: assigned=%s holder=%s, self=%s",
			l.leaderKey, assigned, holder, l.nodeID)
		l.Release()
		return false
	}

	log.Printf("[Failover] Lock acquired for key: %s, lease: %d", l.leaderKey, l.lease)
	go l.keepAlive()
	return true
}

// renewed 记录一次成功续约：start 是请求发起时刻，ttl 是服务端返回的剩余秒数。
func (l *EtcdLock) renewed(start time.Time, ttl int64) {
	l.expireAt.Store(int64(start.Sub(l.base) + time.Duration(ttl)*time.Second))
}

// Valid 返回本地估计下租约是否仍然有效。写入路径在持有 Manager.Mutex 时调用，
// 只读原子变量，不做网络调用。
func (l *EtcdLock) Valid() bool {
	if l.lost.Load() {
		return false
	}
	return time.Since(l.base)+safetyMargin < time.Duration(l.expireAt.Load())
}

// Lost 返回本锁是否已经因续约失败而失效。
func (l *EtcdLock) Lost() bool { return l.lost.Load() }

// keepAlive 每 ttl/3 续约一次。单次续约失败（etcd 选主、网络抖动）不立即降级，
// 而是每 renewRetry 重试一次，直到本地估计的剩余有效期不足 safetyMargin 为止：
// 安全性由 Valid() 保证，失败次数本身不是判断依据。服务端明确返回租约不存在时
// 立即降级，因为此时 key 已被删除，其他节点随时可能拿到锁。
func (l *EtcdLock) keepAlive() {
	interval := time.Duration(l.ttl) * time.Second / 3
	timer := time.NewTimer(interval)
	defer timer.Stop()

	failures := 0
	for {
		select {
		case <-timer.C:
		case <-l.stop:
			return
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), renewTimeout)
		resp, err := l.client.KeepAliveOnce(ctx, l.lease)
		cancel()

		switch {
		case err == nil:
			if failures > 0 {
				log.Printf("[Failover] Lease %d refresh recovered after %d failures", l.lease, failures)
			}
			failures = 0
			l.renewed(start, resp.TTL)
			timer.Reset(interval)
		case errors.Is(err, rpctypes.ErrLeaseNotFound):
			l.markLost(fmt.Sprintf("lease not found on server: %v", err))
			return
		case !l.Valid():
			l.markLost(fmt.Sprintf("lease about to expire after %d failed refreshes, last error: %v", failures+1, err))
			return
		default:
			failures++
			log.Printf("[Failover] Failed to refresh lease %d (attempt %d), retrying: %v", l.lease, failures, err)
			timer.Reset(renewRetry)
		}
	}
}

// markLost 标记锁丢失并触发降级。先置位再回调：晋升流程可能还没把本锁挂到
// Manager 上，它会在挂载前检查这个标记。
func (l *EtcdLock) markLost(reason string) {
	log.Printf("[Failover] Lock lost for key %s: %s", l.leaderKey, reason)
	l.lost.Store(true)
	l.onLost()
}

// Release 停止续约并撤销租约，writeLock key 随 lease 撤销自动删除。可重复调用。
//
// Revoke 带超时：Release 可能在降级路径上调用，不能被一次卡住的 etcd 请求拖住。
// 撤销失败也无妨，lease 会在 TTL 到期后自动过期。
func (l *EtcdLock) Release() {
	l.stopOnce.Do(func() {
		close(l.stop)
		l.lost.Store(true)
		if l.lease == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		defer cancel()
		if _, err := l.client.Revoke(ctx, l.lease); err != nil {
			log.Printf("[Failover] Failed to revoke lease %d for key %s: %v", l.lease, l.leaderKey, err)
			return
		}
		log.Printf("[Failover] Lock released for key: %s, lease: %d", l.leaderKey, l.lease)
	})
}
