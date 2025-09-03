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
		client:      client,
		key:         cfg.Key,
		nodeID:      cfg.NodeID,
		ctx:         ctx,
		cancel:      cancel,
		gracePeriod: cfg.GracePeriod,
		watcher:     clientv3.NewWatcher(client),
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

		lf.LeaderMutex.Lock()
		wasLeader := lf.IsLeaderNode
		lf.LeaderMutex.Unlock()

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

	lf.LeaderMutex.Lock()
	defer lf.LeaderMutex.Unlock()

	// already becomes leader, just return
	if lf.IsLeaderNode {
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