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
)

type LeaderFailover struct {
	client        *clientv3.Client
	key           string
	nodeID        string
	ctx           context.Context
	cancel        context.CancelFunc
	IsLeaderNode  bool
	LeaderMutex   sync.RWMutex
	callbacks     LeaderCallbacks
	gracePeriod   time.Duration
	currentLeader atomic.Value // stores string

	// Write lock key fields
	writeLockKey     string
	writeLockLeaseID atomic.Int64 // Stores clientv3.LeaseID (int64)
	keepAliveCtx     context.Context
	keepAliveCancel  context.CancelFunc
}

func NewLeaderFailover(cfg Config) (*LeaderFailover, error) {
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
	lf.currentLeader.Store("") // Initialize with empty string
	return lf, nil
}

func (lf *LeaderFailover) SetCallbacks(callbacks LeaderCallbacks) {
	lf.callbacks = callbacks
}

func (lf *LeaderFailover) Start() error {
	// initial connection to etcd timeout: 5s
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	defer cancel()

	// Read current leader from etcd
	resp, err := lf.client.Get(ctx, lf.key)
	if err != nil {
		return fmt.Errorf("[Leader Failover] failed to get current leader: %w", err)
	}

	if len(resp.Kvs) > 0 {
		currentLeader := string(resp.Kvs[0].Value)
		lf.currentLeader.Store(currentLeader)
		log.Printf("[Leader Failover] Current leader is %s", currentLeader)

		// Check if this node is the leader
		if currentLeader == lf.nodeID {
			log.Printf("[Leader Failover] Node %s is the current leader in etcd, becoming leader", lf.nodeID)
			lf.becomeLeader()
		} else {
			log.Printf("[Leader Failover] Node %s is in BACKUP mode, current leader is %s", lf.nodeID, currentLeader)
		}
	} else {
		log.Printf("[Leader Failover] No leader set in etcd key %s", lf.key)
		// No leader exists, try to become leader
		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Failed to become leader: %v", err)
		}
	}

	// Start periodic election check
	go lf.startPeriodicElectionCheck()

	return nil
}

func (lf *LeaderFailover) tryToBecomeLeader() error {
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	defer cancel()

	if lf.getCurrentLeader() != "" {
		return nil
	}

	// Try to set ourselves as leader using a transaction to avoid race conditions
	// Use CreateRevision == 0 to check if key doesn't exist (works even after deletion)
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
		// Only update currentLeader, don't call becomeLeader
		// The watch event will handle the actual state transition
		lf.currentLeader.Store(lf.nodeID)
		// Note: becomeLeader() will be called when watch receives the Put event
	} else {
		// Someone else is already leader, update our local state
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				currentLeader := string(rangeResp.Kvs[0].Value)
				lf.currentLeader.Store(currentLeader)
				log.Printf("[Leader Failover] Another node (%s) is already leader", currentLeader)
			}
		}
	}

	return nil
}

func (lf *LeaderFailover) startPeriodicElectionCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Printf("[Leader Failover] Started periodic election check (interval: 5s)")

	for {
		select {
		case <-lf.ctx.Done():
			log.Printf("[Leader Failover] Periodic election check stopped")
			return

		case <-ticker.C:
			// Get current election key state
			resp, err := lf.client.Get(lf.ctx, lf.key)
			if err != nil {
				log.Printf("[Leader Failover] Failed to check election key: %v", err)
				continue
			}

			if len(resp.Kvs) == 0 {
				// Election key does not exist, try to become leader
				log.Printf("[Leader Failover] Election key not found, attempting to become leader")
				if err := lf.tryToBecomeLeader(); err != nil {
					log.Printf("[Leader Failover] Failed to become leader: %v", err)
				}
				continue
			}

			// Election key exists
			currentLeader := string(resp.Kvs[0].Value)
			lf.currentLeader.Store(currentLeader)

			lf.LeaderMutex.RLock()
			isLeader := lf.IsLeaderNode
			lf.LeaderMutex.RUnlock()

			if currentLeader == lf.nodeID {
				// I should be the leader
				if !isLeader {
					// State desync: etcd says I'm leader, but local state says follower
					log.Printf("[Leader Failover] State desync: etcd says leader, local says follower. Syncing...")
					lf.becomeLeader()
				} else {
					// Already leader, check if KeepAlive goroutine is still running
					if lf.writeLockLeaseID.Load() == int64(clientv3.NoLease) {
						// KeepAlive goroutine has stopped, write lock likely lost
						log.Printf("[Leader Failover] KeepAlive not running, stepping down")
						lf.loseLeadership()
					}
				}
			} else {
				// Other node is the leader
				if isLeader {
					// State desync: etcd says other node is leader, but local state says I'm leader
					log.Printf("[Leader Failover] State desync: etcd says follower (leader=%s), local says leader. Stepping down", currentLeader)
					lf.loseLeadership()
				}
				// Already follower, no action needed
			}
		}
	}
}

func (lf *LeaderFailover) becomeLeader() {
	// Wait for the old leader to do cleanup
	log.Printf("[Leader Failover] Current node %s waiting grace period (%v) before becoming leader", lf.nodeID, lf.gracePeriod)
	time.Sleep(lf.gracePeriod)
	// Quick check: if already leader, skip (without holding the lock)
	lf.LeaderMutex.Lock()
	defer lf.LeaderMutex.Unlock()

	if lf.IsLeaderNode && lf.writeLockLeaseID.Load() != int64(clientv3.NoLease) {
		log.Printf("[Leader Failover] Current node %s is already LEADER, skipping", lf.nodeID)
		return
	}

	// Try to acquire write lock key (this ensures only one node can write to Kafka)
	// This method is idempotent, so it's safe to call multiple times
	if err := lf.acquireWriteLockKey(); err != nil {
		log.Printf("[Leader Failover] Current node %s failed to acquire write lock key: %v", lf.nodeID, err)
		// Failed to acquire write lock, do not become leader
		return
	}

	if lf.IsLeaderNode {
		log.Printf("[Leader Failover] Current node %s is already LEADER (double-check), skipping", lf.nodeID)
		return
	}

	lf.IsLeaderNode = true
	log.Printf("[Leader Failover] Current node %s became LEADER", lf.nodeID)

	if err := lf.callbacks.OnBecomeLeader(lf.ctx); err != nil {
		log.Printf("[Leader Failover] Current node %s failed to execute OnBecomeLeader callback: %v", lf.nodeID, err)
	}
}

func (lf *LeaderFailover) loseLeadership() {
	lf.LeaderMutex.Lock()
	defer lf.LeaderMutex.Unlock()

	// Release write lock key first (before executing callback)
	lf.releaseWriteLockKey()

	if !lf.IsLeaderNode {
		return
	}

	// Execute callback for losing leadership
	ctx, cancel := context.WithTimeout(context.Background(), lf.gracePeriod)
	defer cancel()

	if err := lf.callbacks.OnLoseLeader(ctx); err != nil {
		log.Printf("[Leader Failover] Current node %s failed to execute OnLoseLeader callback: %v", lf.nodeID, err)
	}

	lf.IsLeaderNode = false
	log.Printf("[Leader Failover] Current node %s is now in BACKUP mode", lf.nodeID)
}

func (lf *LeaderFailover) IsLeader() bool {
	lf.LeaderMutex.RLock()
	defer lf.LeaderMutex.RUnlock()
	return lf.IsLeaderNode
}

func (lf *LeaderFailover) IsBackup() bool {
	lf.LeaderMutex.RLock()
	defer lf.LeaderMutex.RUnlock()
	return !lf.IsLeader()
}

func (lf *LeaderFailover) getCurrentLeader() string {
	if leader := lf.currentLeader.Load(); leader != nil {
		return leader.(string)
	}
	return ""
}

func (lf *LeaderFailover) Stop() error {
	lf.cancel()
	return nil
}

func (lf *LeaderFailover) Close() error {
	lf.cancel()
	return lf.client.Close()
}

// acquireWriteLockKey tries to acquire the write lock key with unlimited retry
// This method is idempotent: if the key is already held by this node, it returns immediately
func (lf *LeaderFailover) acquireWriteLockKey() error {
	const (
		retryInterval = 50 * time.Millisecond
	)

	log.Printf("[Leader Failover] Node %s attempting to acquire write lock key %s", lf.nodeID, lf.writeLockKey)

	// First, check if we already hold the write lock (idempotency check)
	ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
	resp, err := lf.client.Get(ctx, lf.writeLockKey)
	cancel()

	if err == nil && len(resp.Kvs) > 0 {
		currentHolder := string(resp.Kvs[0].Value)
		if currentHolder == lf.nodeID {
			// This node already holds the write lock
			if lf.writeLockLeaseID.Load() != int64(clientv3.NoLease) {
				// We have the lease ID in memory, just return (idempotent)
				log.Printf("[Leader Failover] Write lock already held by this node, skipping")
				return nil
			} else {
				// Reattach to the existing lease (e.g., after restart)
				leaseID := clientv3.LeaseID(resp.Kvs[0].Lease)
				lf.writeLockLeaseID.Store(int64(leaseID))
				log.Printf("[Leader Failover] Reattaching to existing write lock (lease: %d)", leaseID)
				if leaseID != clientv3.NoLease {
					lf.startKeepAliveWriteLockKey()
				}
				return nil
			}
		}
	}

	// The key doesn't exist or is held by another node, enter acquisition loop
	for retry := 0; ; retry++ {
		// Check if context is cancelled
		select {
		case <-lf.ctx.Done():
			return fmt.Errorf("context cancelled while acquiring write lock: %w", lf.ctx.Err())
		default:
		}

		// Create a lease with TTL
		ctx, cancel := context.WithTimeout(lf.ctx, 5*time.Second)
		leaseResp, err := lf.client.Grant(ctx, writeLockTTL)
		cancel()

		if err != nil {
			log.Printf("[Leader Failover] Failed to create lease for write lock (retry %d): %v", retry+1, err)
			time.Sleep(retryInterval)
			continue
		}

		// Try to acquire the write lock key using a transaction
		// Only succeed if the key doesn't exist (CreateRevision == 0)
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
			// Revoke the lease we just created since we didn't use it
			lf.client.Revoke(context.Background(), leaseResp.ID)
			time.Sleep(retryInterval)
			continue
		}

		if txnResp.Succeeded {
			// Successfully acquired the write lock
			lf.writeLockLeaseID.Store(int64(leaseResp.ID))
			log.Printf("[Leader Failover] Node %s successfully acquired write lock key after %d retries", lf.nodeID, retry+1)

			// Start keepalive for the lease
			lf.startKeepAliveWriteLockKey()
			return nil
		}

		// Failed to acquire, someone else holds the lock
		// Revoke the lease we just created
		lf.client.Revoke(context.Background(), leaseResp.ID)

		// Log who is holding the lock
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

// releaseWriteLockKey releases the write lock key
func (lf *LeaderFailover) releaseWriteLockKey() {
	// Stop keepalive first
	if lf.keepAliveCancel != nil {
		lf.keepAliveCancel()
		lf.keepAliveCancel = nil
	}

	// Delete the write lock key
	leaseID := clientv3.LeaseID(lf.writeLockLeaseID.Load())
	if leaseID != clientv3.NoLease {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Delete the key
		_, err := lf.client.Delete(ctx, lf.writeLockKey)
		if err != nil {
			log.Printf("[Leader Failover] Failed to delete write lock key: %v", err)
		}

		// Revoke the lease
		_, err = lf.client.Revoke(ctx, leaseID)
		if err != nil {
			log.Printf("[Leader Failover] Failed to revoke write lock lease: %v", err)
		}

		lf.writeLockLeaseID.Store(0)
		log.Printf("[Leader Failover] Node %s released write lock key", lf.nodeID)
	}
}

// startKeepAliveWriteLockKey starts a goroutine to keep the write lock lease alive using KeepAliveOnce
func (lf *LeaderFailover) startKeepAliveWriteLockKey() {
	lf.keepAliveCtx, lf.keepAliveCancel = context.WithCancel(lf.ctx)

	go func() {
		const (
			keepAliveInterval = time.Duration(writeLockTTL/4) * time.Second // TTL/4 for safety
			maxFail           = 3                                           // Allow 3 consecutive failures
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
						lf.loseLeadership() // Synchronous call to ensure state is cleaned up
						return
					}
				} else {
					// Success
					if failCount > 0 {
						log.Printf("[Leader Failover] Lease renewal recovered after %d failures", failCount)
					}
					failCount = 0
				}
			}
		}
	}()
}
