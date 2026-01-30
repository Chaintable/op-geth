package leader

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
	watcher       clientv3.Watcher
	currentLeader atomic.Value // stores string

	// Write lock key fields
	writeLockKey     string
	writeLockLeaseID clientv3.LeaseID
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
		watcher:      clientv3.NewWatcher(client),
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

	// Get the revision from the Get response
	// This ensures we don't miss any changes between Get and Watch
	watchRevision := resp.Header.Revision

	if len(resp.Kvs) > 0 {
		currentLeader := string(resp.Kvs[0].Value)
		lf.currentLeader.Store(currentLeader)
		log.Printf("[Leader Failover] Current leader is %s (revision: %d)", currentLeader, watchRevision)

		// Check if this node is the leader
		if currentLeader == lf.nodeID {
			// We are the designated leader in etcd
			// Call becomeLeader immediately since there won't be a watch event for existing state
			log.Printf("[Leader Failover] Node %s is the current leader in etcd, becoming leader", lf.nodeID)
			lf.becomeLeader()
		} else {
			log.Printf("[Leader Failover] Node %s is in BACKUP mode, current leader is %s", lf.nodeID, currentLeader)
		}
	} else {
		log.Printf("[Leader Failover] No leader set in etcd key %s (revision: %d)", lf.key, watchRevision)
		// No leader exists, try to become leader
		if err := lf.tryToBecomeLeader(); err != nil {
			log.Printf("[Leader Failover] Failed to become leader: %v", err)
		}
		// Note: If tryToBecomeLeader succeeds, the watch will receive the Put event and call becomeLeader
	}

	// Start watching for changes from the revision we got from Get
	// This ensures we don't miss any events that happened after our Get
	go lf.watchLeaderChangesFromRevision(watchRevision + 1)

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

func (lf *LeaderFailover) watchLeaderChangesFromRevision(revision int64) {
	// Start watching from the specified revision to avoid missing events
	log.Printf("[Leader Failover] Starting watch from revision %d", revision)
	watchChan := lf.watcher.Watch(lf.ctx, lf.key, clientv3.WithRev(revision))

	for {
		// high priority
		select {
		case <-lf.ctx.Done():
			return
		default:
		}

		select {
		case <-lf.ctx.Done():
			return
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				lf.handleWatchEvent(event)
			}
		}
	}
}

func (lf *LeaderFailover) handleWatchEvent(event *clientv3.Event) {
	switch event.Type {
	case clientv3.EventTypePut:
		newLeader := string(event.Kv.Value)
		oldLeader := lf.getCurrentLeader()
		lf.currentLeader.Store(newLeader)

		log.Printf("[Leader Failover] Leader changed from %s to %s, Current node %s", oldLeader, newLeader, lf.nodeID)

		lf.LeaderMutex.RLock()
		wasLeader := lf.IsLeaderNode
		lf.LeaderMutex.RUnlock()

		if newLeader == lf.nodeID && !wasLeader {
			// This node becomes the leader
			lf.becomeLeader()
		} else if newLeader != lf.nodeID && wasLeader {
			// This node loses leadership
			lf.loseLeadership()
		} else if newLeader != lf.nodeID && !wasLeader {
			// Still backup, just different leader
			log.Printf("[Leader Failover] Current Node %s remains in BACKUP mode, new leader is %s", lf.nodeID, newLeader)
		}

	case clientv3.EventTypeDelete:
		log.Printf("[Leader Failover] Leader key %s was deleted", lf.key)
		oldLeader := lf.getCurrentLeader()
		lf.currentLeader.Store("")

		if lf.IsLeader() {
			// If we were the leader, lose leadership
			lf.loseLeadership()
		}

		// Try to become the new leader after a short delay
		go func() {
			// Wait a random time between 0 and 1s to avoid race conditions with other nodes
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

			log.Printf("[Leader Failover] Key deleted (old leader was %s), attempting to become leader", oldLeader)
			if err := lf.tryToBecomeLeader(); err != nil {
				log.Printf("[Leader Failover] Failed to become leader after key deletion: %v", err)
			}
		}()
	}
}

func (lf *LeaderFailover) becomeLeader() {
	// Wait for the old leader to do cleanup
	log.Printf("[Leader Failover] Current node %s waiting grace period (%v) before becoming leader", lf.nodeID, lf.gracePeriod)
	time.Sleep(lf.gracePeriod)
	// Quick check: if already leader, skip (without holding the lock)
	lf.LeaderMutex.RLock()
	alreadyLeader := lf.IsLeaderNode
	lf.LeaderMutex.RUnlock()

	if alreadyLeader {
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

	// Double-check: acquire the lock and check again
	lf.LeaderMutex.Lock()
	defer lf.LeaderMutex.Unlock()

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

	if !lf.IsLeaderNode {
		return
	}

	// Release write lock key first (before executing callback)
	lf.releaseWriteLockKey()

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
	if err := lf.watcher.Close(); err != nil {
		return err
	}
	return lf.client.Close()
}

// acquireWriteLockKey tries to acquire the write lock key with unlimited retry
// This method is idempotent: if the key is already held by this node, it returns immediately
func (lf *LeaderFailover) acquireWriteLockKey() error {
	const (
		retryInterval = 1 * time.Second
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
			if lf.writeLockLeaseID != 0 {
				// We have the lease ID in memory, just return (idempotent)
				log.Printf("[Leader Failover] Write lock already held by this node, skipping")
				return nil
			} else {
				// Reattach to the existing lease (e.g., after restart)
				lf.writeLockLeaseID = clientv3.LeaseID(resp.Kvs[0].Lease)
				log.Printf("[Leader Failover] Reattaching to existing write lock (lease: %d)", lf.writeLockLeaseID)
				if lf.writeLockLeaseID != 0 {
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
			lf.writeLockLeaseID = leaseResp.ID
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
	if lf.writeLockLeaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Delete the key
		_, err := lf.client.Delete(ctx, lf.writeLockKey)
		if err != nil {
			log.Printf("[Leader Failover] Failed to delete write lock key: %v", err)
		}

		// Revoke the lease
		_, err = lf.client.Revoke(ctx, lf.writeLockLeaseID)
		if err != nil {
			log.Printf("[Leader Failover] Failed to revoke write lock lease: %v", err)
		}

		lf.writeLockLeaseID = 0
		log.Printf("[Leader Failover] Node %s released write lock key", lf.nodeID)
	}
}

// startKeepAliveWriteLockKey starts a goroutine to keep the write lock lease alive using KeepAliveOnce
func (lf *LeaderFailover) startKeepAliveWriteLockKey() {
	lf.keepAliveCtx, lf.keepAliveCancel = context.WithCancel(lf.ctx)

	go func() {
		keepAliveInterval := time.Duration(writeLockTTL/4) * time.Second // TTL/4 for safety

		ticker := time.NewTicker(keepAliveInterval)
		defer ticker.Stop()

		log.Printf("[Leader Failover] Started keepalive for write lock key (interval: %v)", keepAliveInterval)

		for {
			select {
			case <-lf.keepAliveCtx.Done():
				log.Printf("[Leader Failover] Keepalive for write lock key stopped")
				return

			case <-ticker.C:
				ctx, cancel := context.WithTimeout(lf.keepAliveCtx, 3*time.Second)
				resp, err := lf.client.KeepAliveOnce(ctx, lf.writeLockLeaseID)
				cancel()

				if err != nil {
					log.Printf("[Leader Failover] Failed to renew lease: %v", err)
				} else if resp.TTL <= 0 {
					log.Printf("[Leader Failover] WARNING: Lease expired (TTL=%d)", resp.TTL)
				}
			}
		}
	}()
}