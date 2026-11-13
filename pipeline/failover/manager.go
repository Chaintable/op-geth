package failover

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// GlobalManager 是写节点主备状态的全局入口。各链的 tracer 在初始化时赋值，
// 写入路径（Kafka / S3）通过它判断自己是否为主节点。
var GlobalManager *Manager

const (
	defaultLockTTL       = 20
	defaultCheckInterval = 5 * time.Second
	dialTimeout          = 5 * time.Second
	readTimeout          = 5 * time.Second
	// renewTimeout 是单次续约的超时，renewRetry 是续约失败后的重试间隔。
	renewTimeout = 2 * time.Second
	renewRetry   = time.Second
	// safetyMargin 是写入路径在本地估计的租约到期前提前停写的余量，
	// 用来覆盖降级回调尚未执行的窗口。
	safetyMargin = 5 * time.Second
	// minLockTTL 保证续约失败后至少还有几秒的重试窗口：
	// TTL - TTL/3 - safetyMargin > renewTimeout + renewRetry。
	minLockTTL = 12
)

// Config 是主备切换的配置。
type Config struct {
	// EtcdEndpoints 为空且 IsBackup 非空时进入固定模式（不接触 etcd）。
	EtcdEndpoints []string
	// LeaderKey 是运维指派主节点的 key，形如 <chain_id>/<version>/writers/leader
	// 或无版本的 <chain_id>/writers/leader。它的值是被指派节点的 NodeID。
	LeaderKey string
	// NodeID 是本节点标识，与 LeaderKey 的值比对来决定是否应当成为主节点。
	NodeID string
	// IsBackup 非 nil 时启用固定模式：true 恒为备，false 恒为主，不做选举。
	IsBackup *bool
	// LockTTL 是 writeLock 的租约时长（秒），同时决定旧主宕机后的接管延迟。
	LockTTL int64
	// CheckInterval 是轮询 LeaderKey 的间隔。
	CheckInterval time.Duration
	// OnBecomeLeader 在拿到 writeLock 之后、对外宣告成为主节点之前执行，
	// 用于把写入位点对齐到 Kafka 上最后一条消息。返回错误则放弃本次晋升。
	// 必填，由 SetupFailover 设置。
	OnBecomeLeader func() error
	// OnLoseLeader 在失去主节点身份后执行。必填，由 SetupFailover 设置。
	OnLoseLeader func() error
}

// Manager 负责写节点的主备切换。
//
// 并发模型只有一把锁 Mutex，它同时承担两件事：作为 Kafka 写入的闸门，以及保护
// lock 字段。因此持有它的临界区必须极短，绝不允许在持有期间做网络调用。
//
// Mutex 故意导出：写入路径（processor 包）必须在判角色前后持有它，直接写
// GlobalManager.Mutex.Lock() 比包一层同名方法更清楚，也让 Mutex 的全部使用点一次可查。
//
// 抢锁（可能阻塞在 watch 上）和 OnBecomeLeader 回调（要读 Kafka，实测可达秒级）
// 都在不持有 Mutex 的情况下执行。抢到 etcd 锁之后、本地状态置为主之前的这段窗口内，
// 本节点对外仍报告「非主」，只会延迟自己的写入，不会造成双写。
type Manager struct {
	config Config

	client       *clientv3.Client
	leaderKey    string // <chain_id>/[<version>/]writers/leader，运维指派
	writeLockKey string // <leaderKey>/writeLock，主节点持有

	// Mutex 既是 Kafka 写入的闸门，也保护 lock 字段。写入路径直接持有它，
	// 因此必须导出；角色切换要改动 lock 也得先拿到它。
	Mutex sync.RWMutex
	lock  *EtcdLock // 非 nil 表示本节点持有 writeLock

	// 固定模式
	manualMode   bool
	manualBackup bool

	quit      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewManager 创建 Manager。固定模式下不连接 etcd。
func NewManager(cfg Config) (*Manager, error) {
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = defaultLockTTL
	}
	if cfg.LockTTL < minLockTTL {
		return nil, fmt.Errorf("lock ttl must be >= %d seconds, got %d", minLockTTL, cfg.LockTTL)
	}
	if cfg.LockTTL > 60 {
		return nil, fmt.Errorf("lock ttl must be <= 60 seconds, got %d", cfg.LockTTL)
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = defaultCheckInterval
	}
	// 两个回调由 SetupFailover 统一设置，在这里一次性拦住，
	// 避免直接构造 Config 时漏设导致运行期 panic。
	if cfg.OnBecomeLeader == nil || cfg.OnLoseLeader == nil {
		return nil, fmt.Errorf("OnBecomeLeader and OnLoseLeader must not be nil")
	}

	m := &Manager{
		config: cfg,
		quit:   make(chan struct{}),
	}

	// 固定模式：不需要 etcd
	if cfg.IsBackup != nil {
		m.manualMode = true
		m.manualBackup = *cfg.IsBackup
		log.Printf("[Failover] NodeID %s created in fixed mode, isBackup=%v", cfg.NodeID, m.manualBackup)
		return m, nil
	}

	if len(cfg.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints must not be empty in auto mode")
	}
	if cfg.LeaderKey == "" {
		return nil, fmt.Errorf("leader key must not be empty in auto mode")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node id must not be empty in auto mode")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	m.client = client
	m.leaderKey = cfg.LeaderKey
	m.writeLockKey = cfg.LeaderKey + "/writeLock"
	log.Printf("[Failover] NodeID %s created in auto mode, leaderKey=%s writeLockKey=%s ttl=%ds",
		cfg.NodeID, m.leaderKey, m.writeLockKey, cfg.LockTTL)
	return m, nil
}

// Start 执行一次初始判定并启动周期性检查。
func (m *Manager) Start() error {
	if m.manualMode {
		if m.manualBackup {
			return nil
		}
		// 固定主节点：仍要对齐写入位点，否则会基于过期位点推送。
		if err := m.config.OnBecomeLeader(); err != nil {
			return fmt.Errorf("fixed leader failed to align write position: %w", err)
		}
		return nil
	}
	return m.InitLeaderFromEtcd()
}

// InitLeaderFromEtcd 读取一次指派 key 决定初始角色，然后启动周期性检查。
func (m *Manager) InitLeaderFromEtcd() error {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	resp, err := m.client.Get(ctx, m.leaderKey)
	cancel()
	if err != nil {
		return fmt.Errorf("get leader key from etcd: %w", err)
	}

	if len(resp.Kvs) > 0 {
		assigned := string(resp.Kvs[0].Value)
		if assigned == m.config.NodeID {
			log.Printf("[Failover] Leader key matches, becoming leader: %s", m.config.NodeID)
			if err := m.ChangeToChainLeader(); err != nil {
				// 首次抢锁失败不阻塞启动：旧主的 lease 可能还没过期，
				// 周期性检查会继续重试。
				log.Printf("[Failover] Initial promotion failed, will retry: %v", err)
			}
		} else {
			log.Printf("[Failover] Node %s, leader is %s, staying backup", m.config.NodeID, assigned)
		}
	} else {
		log.Printf("[Failover] Leader key does not exist in etcd: %s", m.leaderKey)
	}

	m.wg.Add(1)
	go m.watchLeaderKey()
	return nil
}

// watchLeaderKey 周期性比对指派 key 与本节点身份，驱动晋升和降级。
func (m *Manager) watchLeaderKey() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	log.Printf("[Failover] Starting periodic check for leader key: %s (interval: %v)",
		m.leaderKey, m.config.CheckInterval)

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
			resp, err := m.client.Get(ctx, m.leaderKey)
			cancel()
			if err != nil {
				// 读不到 etcd 不改变本地角色：已持有的 writeLock 由 keepAlive
				// 负责续约，续不上才降级。
				log.Printf("[Failover] Failed to get leader key: %v", err)
				continue
			}

			if len(resp.Kvs) == 0 {
				// 指派 key 不存在，放弃主节点身份
				if m.IsLeader() {
					log.Printf("[Failover] Leader key does not exist, revoking leader")
					m.RevokeChainLeader()
				}
				continue
			}

			assigned := string(resp.Kvs[0].Value)
			if assigned == m.config.NodeID {
				// 指派匹配，尝试成为主节点
				if !m.IsLeader() {
					log.Printf("[Failover] Leader key matches, attempting to become leader: %s", m.config.NodeID)
					if err := m.ChangeToChainLeader(); err != nil {
						log.Printf("[Failover] Failed to become chain leader: %v", err)
					}
				}
			} else {
				// 指派已切走，放弃主节点身份
				if m.IsLeader() {
					log.Printf("[Failover] Leader reassigned (expected: %s, got: %s), revoking leader",
						m.config.NodeID, assigned)
					m.RevokeChainLeader()
				}
			}

		case <-m.quit:
			log.Printf("[Failover] Stopping periodic check for leader key: %s", m.leaderKey)
			return
		}
	}
}

// ChangeToChainLeader 抢占 writeLock 并晋升为主节点。
//
// 抢锁与 OnBecomeLeader 回调都在不持有 mu 的情况下执行，避免阻塞区块提交。
//
// 调用约定：只允许 watchLeaderKey 这一个 goroutine 调用（启动时的首次调用发生在
// 该 goroutine 启动之前），因此内部无需再防并发晋升。若将来从别处并发调用，
// 两个流程可能各自持有一把 etcd 锁并互相覆盖 m.lock，导致租约泄漏。
func (m *Manager) ChangeToChainLeader() error {
	if m.IsLeader() {
		return nil
	}

	etcdLock := NewLock(
		m.writeLockKey,
		m.leaderKey,
		m.config.NodeID,
		m.client,
		m.config.LockTTL,
		m.RevokeChainLeader,
	)

	// 抢锁有网络调用，不能持有 Mutex。拿不到就等下一个检查周期重试。
	if !etcdLock.Acquire() {
		log.Printf("[Failover] acquire etcd lock failed")
		return fmt.Errorf("acquire etcd lock failed")
	}
	log.Printf("[Failover] acquire etcd lock success")

	// 已持有 etcd 锁但本地仍报告非主，此时对齐写入位点。
	// 这一步要读 Kafka，耗时可达秒级，同样不能持有 Mutex。
	if err := m.config.OnBecomeLeader(); err != nil {
		log.Printf("[Failover] OnBecomeLeader failed, releasing lock: %v", err)
		etcdLock.Release()
		return fmt.Errorf("on become leader: %w", err)
	}

	// 位点已对齐，正式宣告成为主节点。临界区只有判断和赋值，没有网络调用。
	//
	// 必须在同一临界区内确认锁没有在上面那段耗时窗口里丢失：keepAlive 从 Acquire
	// 成功那一刻就开始续约，若续约在 OnBecomeLeader 期间失败，它会把锁标记为丢失
	// 并回调 RevokeChainLeader，而那时 m.lock 仍是 nil，Revoke 会直接返回。
	// 此处不检查就赋值的话，本节点会带着一个已死的租约永久自认为主节点。
	//
	// Lost 置位发生在 Revoke 尝试获取 Mutex 之前，因此这里读到 false 就意味着
	// Revoke 必然排在本次赋值之后，它随后能看到并正确释放这把锁。
	m.Mutex.Lock()
	if !etcdLock.Valid() {
		m.Mutex.Unlock()
		log.Printf("[Failover] writeLock lost during promotion, aborting for node %s", m.config.NodeID)
		etcdLock.Release()
		return fmt.Errorf("writeLock lost during promotion")
	}
	m.lock = etcdLock
	m.Mutex.Unlock()

	log.Printf("[Failover] Node %s became LEADER with writeLock", m.config.NodeID)
	return nil
}

// RevokeChainLeader 释放 writeLock 并降级为备节点。
//
// 它同时是续约失败时的回调：租约在本地估计下即将到期或已在服务端消失时降级。
func (m *Manager) RevokeChainLeader() {
	m.Mutex.Lock()
	lock := m.lock
	m.lock = nil
	m.Mutex.Unlock()

	if lock == nil {
		return
	}

	// 本地状态已置为备，写入闸门已关闭，此处再做 etcd 撤销，不阻塞写入路径。
	lock.Release()
	log.Printf("[Failover] release etcd lock success, node %s is now BACKUP", m.config.NodeID)

	if err := m.config.OnLoseLeader(); err != nil {
		log.Printf("[Failover] OnLoseLeader failed: %v", err)
	}
}

// IsLeader 返回本节点当前是否为主节点。
func (m *Manager) IsLeader() bool {
	if m.manualMode {
		return !m.manualBackup
	}
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	return m.lock != nil
}

// IsBackup 返回本节点当前是否为备节点。
func (m *Manager) IsBackup() bool { return !m.IsLeader() }

// IsLeaderLocked 供已持有 Mutex 的写入路径使用。此时不能再加锁（RWMutex 不可
// 重入），直接读即可：角色切换必须先拿到同一把 Mutex 才能改动 lock，因此这保证了
// 「判角色」与「写 Kafka」相对于角色切换是原子的。调用前必须持有 Mutex。
func (m *Manager) IsLeaderLocked() bool {
	if m.manualMode {
		return !m.manualBackup
	}
	// 租约按本地估计已到期时，即使降级回调还没跑完也必须停写。
	return m.lock != nil && m.lock.Valid()
}

// Close 停止周期性检查并释放 writeLock。
func (m *Manager) Close() error {
	var err error
	m.closeOnce.Do(func() {
		close(m.quit)
		m.wg.Wait()
		if !m.manualMode {
			m.RevokeChainLeader()
			if m.client != nil {
				err = m.client.Close()
			}
		}
	})
	return err
}

// BuildLeaderKey 按约定拼出指派 key：
//
//	有版本：<chain_id>/<version>/writers/leader
//	无版本：<chain_id>/writers/leader
func BuildLeaderKey(chainID, version string) string {
	if version == "" {
		return fmt.Sprintf("%s/writers/leader", chainID)
	}
	return fmt.Sprintf("%s/%s/writers/leader", chainID, version)
}

// IsLeaderNode 是给写入路径用的便捷判断，GlobalManager 未初始化时返回 false。
func IsLeaderNode() bool {
	return GlobalManager != nil && GlobalManager.IsLeader()
}
