package leader

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// writeLockTTL is the TTL for the write lock lease in seconds
	writeLockTTL = 15

	healthCheckInterval = 5 * time.Second
	healthCheckTimeout  = 3 * time.Second
	reconcileBaseDelay  = 250 * time.Millisecond
	reconcileMaxDelay   = 10 * time.Second
)

// State represents the current leadership state of the node.
// StateUnknown is distinct from StateBackup: when etcd is unreachable the node
// cannot confirm it is still the leader, so Kafka writes are denied.
type State int32

const (
	StateUnknown State = iota
	StateBackup
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateUnknown:
		return "Unknown"
	case StateBackup:
		return "Backup"
	case StateLeader:
		return "Leader"
	default:
		return "Invalid"
	}
}

type LeaderFailover struct {
	client   *clientv3.Client
	key      string
	nodeID   string
	ctx      context.Context
	cancel   context.CancelFunc
	callbacks LeaderCallbacks
	gracePeriod time.Duration

	// state is the authoritative source-of-truth for leadership.
	// Access only via transition() or markUnknown(), read via atomicState().
	state atomic.Int32

	// LeaderMutex guards the Kafka write path; held as Write-lock when
	// loseLeadership() is executing so in-flight writes drain first.
	LeaderMutex sync.RWMutex
	roleUpdateMu sync.Mutex

	// promotionToken is incremented whenever the etcd leader key changes.
	// becomeLeader() captures the token before sleeping the grace period and
	// aborts if the token has changed by the time it wakes up.
	promotionToken atomic.Int64

	// currentLeader is the last known etcd leader value.
	leaderValueMu  sync.Mutex
	currentLeader  string
	currentRevision int64

	// etcdHealthy tracks whether the last health-check succeeded.
	etcdHealthy atomic.Bool

	// IsLeaderNode is kept for backward-compat with callers that read it
	// under LeaderMutex. It mirrors state == StateLeader.
	IsLeaderNode bool

	// Write lock key fields
	writeLockKey     string
	writeLockLeaseID atomic.Int64 // Stores clientv3.LeaseID (int64)
	keepAliveCtx     context.Context
	keepAliveCancel  context.CancelFunc

	closeOnce sync.Once
}

func NewLeaderFailover(cfg Config) (*LeaderFailover, error) {
	if cfg.GracePeriod <= healthCheckInterval+healthCheckTimeout {
		log.Printf("[Leader Failover] WARNING: GracePeriod (%v) should be > healthCheckInterval+healthCheckTimeout (%v) for safe failover",
			cfg.GracePeriod, healthCheckInterval+healthCheckTimeout,
		)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	lf := &LeaderFailover{
		client:       client,
		key:          cfg.Key,
		nodeID:       cfg.NodeID,
		ctx:          ctx,
		cancel:       cancel,
		gracePeriod:  cfg.GracePeriod,
		writeLockKey: cfg.Key + "/write-lock",
	}
	lf.state.Store(int32(StateUnknown))
	lf.etcdHealthy.Store(false)
	return lf, nil
}

func (lf *LeaderFailover) SetCallbacks(callbacks LeaderCallbacks) {
	lf.callbacks = callbacks
}

// atomicState returns the current state without locks.
func (lf *LeaderFailover) atomicState() State {
	return State(lf.state.Load())
}

// transition moves the node to newState, executing callbacks as needed.
// Must be called with roleUpdateMu held.
func (lf *LeaderFailover) transition(newState State) {
	old := lf.atomicState()
	if old == newState {
		return
	}

	log.Printf("[Leader Failover] Node %s: %s → %s", lf.nodeID, old, newState)

	if old == StateLeader && newState != StateLeader {
		// We are leaving leader state — execute loss callback while still marking
		// ourselves as leader so in-flight writes can complete.
		lf.LeaderMutex.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), lf.gracePeriod)
		defer cancel()
		if err := lf.callbacks.OnLoseLeader(ctx); err != nil {
			log.Printf("[Leader Failover] OnLoseLeader error: %v", err)
		}
		lf.IsLeaderNode = false
		lf.state.Store(int32(newState))
		lf.releaseWriteLockKey()
		lf.LeaderMutex.Unlock()
	} else {
		lf.state.Store(int32(newState))
		if newState == StateLeader {
			lf.LeaderMutex.Lock()
			lf.IsLeaderNode = true
			lf.LeaderMutex.Unlock()
		} else {
			lf.LeaderMutex.Lock()
			lf.IsLeaderNode = false
			lf.LeaderMutex.Unlock()
		}
	}
}

// markUnknown transitions to StateUnknown regardless of current state.
func (lf *LeaderFailover) markUnknown() {
	lf.roleUpdateMu.Lock()
	defer lf.roleUpdateMu.Unlock()
	lf.transition(StateUnknown)
}

func (lf *LeaderFailover) Start() error {
	// initial connection to etcd timeout: 5s
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	defer cancel()

	resp, err := lf.client.Get(ctx, lf.key)
	if err != nil {
		return fmt.Errorf("[Leader Failover] failed to get current leader: %w", err)
	}

	lf.etcdHealthy.Store(true)

	if len(resp.Kvs) > 0 {
		currentLeader := string(resp.Kvs[0].Value)
		lf.setLeaderValue(currentLeader, resp.Header.Revision)
		log.Printf("[Leader Failover] Current leader is %s", currentLeader)

		if currentLeader == lf.nodeID {
			log.Printf("[Leader Failover] Node %s is the current leader in etcd, becoming leader", lf.nodeID)
			lf.becomeLeader()
		} else {
			lf.roleUpdateMu.Lock()
			lf.transition(StateBackup)
			lf.roleUpdateMu.Unlock()
			log.Printf("[Leader Failover] Node %s is in BACKUP mode, current leader is %s", lf.nodeID, currentLeader)
		}
	} else {
		log.Printf("[Leader Failover] No leader set in etcd key %s", lf.key)
		lf.roleUpdateMu.Lock()
		lf.transition(StateBackup)
		lf.roleUpdateMu.Unlock()
		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Failed to become leader: %v", err)
		}
	}

	go lf.watchLoop()

	return nil
}

func (lf *LeaderFailover) setLeaderValue(leader string, revision int64) {
	lf.leaderValueMu.Lock()
	defer lf.leaderValueMu.Unlock()
	if revision >= lf.currentRevision {
		lf.currentLeader = leader
		lf.currentRevision = revision
	}
}

func (lf *LeaderFailover) getCurrentLeader() string {
	lf.leaderValueMu.Lock()
	defer lf.leaderValueMu.Unlock()
	return lf.currentLeader
}

func (lf *LeaderFailover) tryToBecomeLeader() error {
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	defer cancel()

	if lf.getCurrentLeader() != "" {
		return nil
	}

	txn := lf.client.Txn(ctx)
	txnResp, err := txn.If(
		clientv3.Compare(clientv3.CreateRevision(lf.key), "=", 0),
	).Then(
		clientv3.OpPut(lf.key, lf.nodeID),
	).Else(
		clientv3.OpGet(lf.key),
	).Commit()

	if err != nil {
		return fmt.Errorf("failed to set leader: %w", err)
	}

	if txnResp.Succeeded {
		log.Printf("[Leader Failover] Successfully set leader key to %s in etcd", lf.nodeID)
		lf.setLeaderValue(lf.nodeID, txnResp.Header.Revision)
		lf.promotionToken.Add(1)
		// becomeLeader will be called by the watch event
	} else {
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				currentLeader := string(rangeResp.Kvs[0].Value)
				lf.setLeaderValue(currentLeader, txnResp.Header.Revision)
				log.Printf("[Leader Failover] Another node (%s) is already leader", currentLeader)
			}
		}
	}

	return nil
}

// watchLoop watches the election key and handles health-check ticks.
// On error it calls reconcile() with exponential backoff before restarting.
func (lf *LeaderFailover) watchLoop() {
	delay := reconcileBaseDelay

	for {
		select {
		case <-lf.ctx.Done():
			return
		default:
		}

		if err := lf.runWatch(); err != nil {
			log.Printf("[Leader Failover] Watch error: %v, reconciling in %v", err, delay)
			lf.reconcile()
			select {
			case <-lf.ctx.Done():
				return
			case <-time.After(delay):
			}
			delay *= 2
			if delay > reconcileMaxDelay {
				delay = reconcileMaxDelay
			}
		} else {
			delay = reconcileBaseDelay
		}
	}
}

// runWatch runs a single watch session including the health-check ticker.
// Returns an error if the watch channel closes unexpectedly.
func (lf *LeaderFailover) runWatch() error {
	watchCh := lf.client.Watch(lf.ctx, lf.key, clientv3.WithPrevKV())
	healthTicker := time.NewTicker(healthCheckInterval)
	defer healthTicker.Stop()

	for {
		select {
		case <-lf.ctx.Done():
			return nil

		case <-healthTicker.C:
			lf.healthCheck()

		case resp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if resp.Err() != nil {
				return resp.Err()
			}
			lf.etcdHealthy.Store(true)

			for _, event := range resp.Events {
				lf.handleWatchEvent(event, resp.Header.Revision)
			}
		}
	}
}

func (lf *LeaderFailover) handleWatchEvent(event *clientv3.Event, revision int64) {
	switch event.Type {
	case clientv3.EventTypePut:
		newLeader := string(event.Kv.Value)
		lf.setLeaderValue(newLeader, revision)
		lf.promotionToken.Add(1)
		log.Printf("[Leader Failover] Watch: leader key set to %s", newLeader)

		if newLeader == lf.nodeID {
			go lf.becomeLeader()
		} else {
			lf.roleUpdateMu.Lock()
			if lf.atomicState() == StateLeader {
				lf.transition(StateBackup)
			} else {
				lf.transition(StateBackup)
			}
			lf.roleUpdateMu.Unlock()
		}

	case clientv3.EventTypeDelete:
		log.Printf("[Leader Failover] Watch: leader key deleted")
		lf.setLeaderValue("", revision)
		lf.promotionToken.Add(1)

		lf.roleUpdateMu.Lock()
		if lf.atomicState() == StateLeader {
			lf.transition(StateBackup)
		}
		lf.roleUpdateMu.Unlock()

		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Failed to become leader after key deletion: %v", err)
		}
	}
}

// healthCheck performs a lightweight etcd ping and syncs current revision.
// On failure the node transitions to StateUnknown.
func (lf *LeaderFailover) healthCheck() {
	ctx, cancel := context.WithTimeout(lf.ctx, healthCheckTimeout)
	defer cancel()

	resp, err := lf.client.Get(ctx, lf.key)
	if err != nil {
		log.Printf("[Leader Failover] Health check failed: %v", err)
		lf.etcdHealthy.Store(false)
		lf.markUnknown()
		return
	}

	lf.etcdHealthy.Store(true)

	if len(resp.Kvs) > 0 {
		currentLeader := string(resp.Kvs[0].Value)
		lf.setLeaderValue(currentLeader, resp.Header.Revision)

		lf.roleUpdateMu.Lock()
		cur := lf.atomicState()
		if currentLeader == lf.nodeID {
			if cur == StateBackup || cur == StateUnknown {
				log.Printf("[Leader Failover] Health-check desync: etcd says leader, local says %s", cur)
				go lf.becomeLeader()
			} else if cur == StateLeader {
				if lf.writeLockLeaseID.Load() == int64(clientv3.NoLease) {
					log.Printf("[Leader Failover] Health-check: keepalive not running, stepping down")
					lf.transition(StateBackup)
				}
			}
		} else {
			if cur == StateLeader {
				log.Printf("[Leader Failover] Health-check desync: etcd says follower (leader=%s), stepping down", currentLeader)
				lf.transition(StateBackup)
			} else if cur == StateUnknown {
				lf.transition(StateBackup)
			}
		}
		lf.roleUpdateMu.Unlock()
	} else {
		// No leader — try to become one
		lf.roleUpdateMu.Lock()
		if lf.atomicState() == StateLeader {
			lf.transition(StateBackup)
		} else if lf.atomicState() == StateUnknown {
			lf.transition(StateBackup)
		}
		lf.roleUpdateMu.Unlock()
		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Failed to become leader during health-check: %v", err)
		}
	}
}

// reconcile re-reads etcd state after a watch error.
func (lf *LeaderFailover) reconcile() {
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	defer cancel()

	resp, err := lf.client.Get(ctx, lf.key)
	if err != nil {
		log.Printf("[Leader Failover] Reconcile failed: %v", err)
		lf.etcdHealthy.Store(false)
		lf.markUnknown()
		return
	}

	lf.etcdHealthy.Store(true)

	if len(resp.Kvs) > 0 {
		currentLeader := string(resp.Kvs[0].Value)
		lf.setLeaderValue(currentLeader, resp.Header.Revision)

		lf.roleUpdateMu.Lock()
		if currentLeader == lf.nodeID {
			if lf.atomicState() != StateLeader {
				log.Printf("[Leader Failover] Reconcile: should be leader, re-promoting")
				go lf.becomeLeader()
			}
		} else {
			if lf.atomicState() == StateLeader {
				lf.transition(StateBackup)
			} else {
				lf.transition(StateBackup)
			}
		}
		lf.roleUpdateMu.Unlock()
	} else {
		lf.roleUpdateMu.Lock()
		if lf.atomicState() != StateBackup {
			lf.transition(StateBackup)
		}
		lf.roleUpdateMu.Unlock()
		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Reconcile: failed to become leader: %v", err)
		}
	}
}

func (lf *LeaderFailover) becomeLeader() {
	// Capture promotion token before grace-period sleep so we can detect stale promotions.
	tokenBefore := lf.promotionToken.Load()

	log.Printf("[Leader Failover] Node %s waiting grace period (%v) before becoming leader", lf.nodeID, lf.gracePeriod)
	timer := time.NewTimer(lf.gracePeriod)
	defer timer.Stop()

	select {
	case <-lf.ctx.Done():
		return
	case <-timer.C:
	}

	// If the etcd key changed while we were sleeping, abort.
	if lf.promotionToken.Load() != tokenBefore {
		log.Printf("[Leader Failover] Node %s: promotion token changed during grace period, aborting", lf.nodeID)
		return
	}

	lf.roleUpdateMu.Lock()
	defer lf.roleUpdateMu.Unlock()

	if lf.atomicState() == StateLeader && lf.writeLockLeaseID.Load() != int64(clientv3.NoLease) {
		log.Printf("[Leader Failover] Node %s is already LEADER, skipping", lf.nodeID)
		return
	}

	if err := lf.acquireWriteLockKey(); err != nil {
		log.Printf("[Leader Failover] Node %s failed to acquire write lock key: %v", lf.nodeID, err)
		return
	}

	lf.transition(StateLeader)
	log.Printf("[Leader Failover] Node %s became LEADER", lf.nodeID)

	if err := lf.callbacks.OnBecomeLeader(lf.ctx); err != nil {
		log.Printf("[Leader Failover] OnBecomeLeader error: %v", err)
	}
}

// loseLeadership is kept for backward-compat with the keepalive path.
func (lf *LeaderFailover) loseLeadership() {
	lf.roleUpdateMu.Lock()
	defer lf.roleUpdateMu.Unlock()
	lf.transition(StateBackup)
}

// IsLeader returns true only when the node is in StateLeader.
func (lf *LeaderFailover) IsLeader() bool {
	lf.LeaderMutex.RLock()
	defer lf.LeaderMutex.RUnlock()
	return lf.IsLeaderNode
}

// IsLeaderLocked is like IsLeader but assumes LeaderMutex is already held
// (either read or write lock) by the caller.
func (lf *LeaderFailover) IsLeaderLocked() bool {
	return lf.atomicState() == StateLeader
}

// IsBackup returns true when the node is NOT the leader (including Unknown).
func (lf *LeaderFailover) IsBackup() bool {
	return !lf.IsLeader()
}

func (lf *LeaderFailover) Stop() error {
	lf.cancel()
	return nil
}

func (lf *LeaderFailover) Close() error {
	lf.closeOnce.Do(func() {
		lf.markUnknown()
		lf.cancel()
	})
	return lf.client.Close()
}

// acquireWriteLockKey tries to acquire the write lock key with unlimited retry.
func (lf *LeaderFailover) acquireWriteLockKey() error {
	const retryInterval = 50 * time.Millisecond

	log.Printf("[Leader Failover] Node %s attempting to acquire write lock key %s", lf.nodeID, lf.writeLockKey)

	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	resp, err := lf.client.Get(ctx, lf.writeLockKey)
	cancel()

	if err == nil && len(resp.Kvs) > 0 {
		currentHolder := string(resp.Kvs[0].Value)
		if currentHolder == lf.nodeID {
			if lf.writeLockLeaseID.Load() != int64(clientv3.NoLease) {
				log.Printf("[Leader Failover] Write lock already held by this node, skipping")
				return nil
			}
			leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
			lf.writeLockLeaseID.Store(int64(leaseID))
			log.Printf("[Leader Failover] Reattaching to existing write lock (lease: %d)", leaseID)
			if leaseID != clientv3.NoLease {
				lf.startKeepAliveWriteLockKey()
			}
			return nil
		}
	}

	for retry := 0; ; retry++ {
		select {
		case <-lf.ctx.Done():
			return fmt.Errorf("context cancelled while acquiring write lock: %w", lf.ctx.Err())
		default:
		}

		ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
		leaseResp, err := lf.client.Grant(ctx, writeLockTTL)
		cancel()

		if err != nil {
			log.Printf("[Leader Failover] Failed to create lease for write lock (retry %d): %v", retry+1, err)
			time.Sleep(retryInterval)
			continue
		}

		ctx, cancel = context.WithTimeout(lf.ctx, 5*time.Second)
		txn := lf.client.Txn(ctx)
		txnResp, err := txn.If(
			clientv3.Compare(clientv3.CreateRevision(lf.writeLockKey), "=", 0),
		).Then(
			clientv3.OpPut(lf.writeLockKey, lf.nodeID, clientv3.WithLease(leaseResp.ID)),
		).Else(
			clientv3.OpGet(lf.writeLockKey),
		).Commit()
		cancel()

		if err != nil {
			log.Printf("[Leader Failover] Failed to acquire write lock (retry %d): %v", retry+1, err)
			lf.client.Revoke(context.Background(), leaseResp.ID)
			time.Sleep(retryInterval)
			continue
		}

		if txnResp.Succeeded {
			lf.writeLockLeaseID.Store(int64(leaseResp.ID))
			log.Printf("[Leader Failover] Node %s successfully acquired write lock key after %d retries", lf.nodeID, retry+1)
			lf.startKeepAliveWriteLockKey()
			return nil
		}

		lf.client.Revoke(context.Background(), leaseResp.ID)

		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				holder := string(rangeResp.Kvs[0].Value)
				log.Printf("[Leader Failover] Write lock key held by %s, retrying in %v (retry %d)", holder, retryInterval, retry+1)
			}
		}

		time.Sleep(retryInterval)
	}
}

// releaseWriteLockKey releases the write lock key.
func (lf *LeaderFailover) releaseWriteLockKey() {
	if lf.keepAliveCancel != nil {
		lf.keepAliveCancel()
		lf.keepAliveCancel = nil
	}

	leaseID := clientv3.LeaseID(lf.writeLockLeaseID.Load())
	if leaseID != clientv3.NoLease {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if _, err := lf.client.Delete(ctx, lf.writeLockKey); err != nil {
			log.Printf("[Leader Failover] Failed to delete write lock key: %v", err)
		}
		if _, err := lf.client.Revoke(ctx, leaseID); err != nil {
			log.Printf("[Leader Failover] Failed to revoke write lock lease: %v", err)
		}

		lf.writeLockLeaseID.Store(0)
		log.Printf("[Leader Failover] Node %s released write lock key", lf.nodeID)
	}
}

// startKeepAliveWriteLockKey starts a goroutine to keep the write lock lease alive.
func (lf *LeaderFailover) startKeepAliveWriteLockKey() {
	lf.keepAliveCtx, lf.keepAliveCancel = context.WithCancel(lf.ctx)

	go func() {
		const (
			keepAliveInterval = time.Duration(writeLockTTL/4) * time.Second
			maxFail           = 3
		)

		failCount := 0
		ticker := time.NewTicker(keepAliveInterval)
		defer ticker.Stop()

		log.Printf("[Leader Failover] Started keepalive for write lock key (interval: %v)", keepAliveInterval)

		for {
			select {
			case <-lf.keepAliveCtx.Done():
				log.Printf("[Leader Failover] Keepalive for write lock key stopped")
				return

			case <-ticker.C:
				leaseID := clientv3.LeaseID(lf.writeLockLeaseID.Load())
				ctx, cancel := context.WithTimeout(lf.keepAliveCtx, 3*time.Second)
				resp, err := lf.client.KeepAliveOnce(ctx, leaseID)
				cancel()

				if err != nil || (resp != nil && resp.TTL <= 0) {
					failCount++
					if err != nil {
						log.Printf("[Leader Failover] Failed to renew lease (%d/%d): %v", failCount, maxFail, err)
					} else {
						log.Printf("[Leader Failover] WARNING: Lease expired (%d/%d, TTL=%d)", failCount, maxFail, resp.TTL)
					}
					if failCount >= maxFail {
						log.Printf("[Leader Failover] Max retries reached, lease likely expired, stepping down")
						lf.loseLeadership()
						return
					}
				} else {
					if failCount > 0 {
						log.Printf("[Leader Failover] Lease renewal recovered after %d failures", failCount)
					}
					failCount = 0
				}
			}
		}
	}()
}
