package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// WriterNodeInfo represents the writer node information stored in etcd
type WriterNodeInfo struct {
	NodeXBucket      string   `json:"node_x_bucket"`
	ChainTableBucket string   `json:"chain_table_bucket"`
	Region           string   `json:"region"`
	Brokers          []string `json:"brokers"`
	Topic            string   `json:"topic"`
}

// WriterRegistry manages writer node registration in etcd
type WriterRegistry struct {
	client   *clientv3.Client
	chainID  string
	version  string
	nodeID   string
	nodeInfo WriterNodeInfo
	lease    clientv3.Lease
	leaseID  clientv3.LeaseID
	ttl      int64
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewWriterRegistry creates a new WriterRegistry instance
func NewWriterRegistry(client *clientv3.Client, chainID, version, nodeID string, nodeInfo WriterNodeInfo, ttl int64) *WriterRegistry {
	ctx, cancel := context.WithCancel(context.Background())

	return &WriterRegistry{
		client:   client,
		chainID:  chainID,
		version:  version,
		nodeID:   nodeID,
		nodeInfo: nodeInfo,
		ttl:      ttl,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// RegisterNode registers the writer node in etcd with a lease
func (wr *WriterRegistry) RegisterNode() error {
	// Grant lease and register key
	if err := wr.grantLeaseAndRegisterKey(); err != nil {
		// For initial registration, panic if node already exists
		if err.Error() == "node already exists" {
			panic(fmt.Sprintf("[Writer Registry] Node with ID %s already exists for chain %s", wr.nodeID, wr.chainID))
		}
		return err
	}

	// Keep lease alive using KeepAliveOnce with retry
	go wr.startKeepAlive()

	log.Printf("[Writer Registry] Node %s registered successfully for chain %s with lease %d",
		wr.nodeID, wr.chainID, wr.leaseID)
	return nil
}

// grantLeaseAndRegisterKey grants a new lease and registers the node key with transaction
func (wr *WriterRegistry) grantLeaseAndRegisterKey() error {
	if wr.leaseID != clientv3.NoLease {
		wr.lease.Revoke(context.Background(), wr.leaseID)
		wr.leaseID = clientv3.NoLease
	}
	if wr.lease != nil {
		wr.lease.Close()
	}
	wr.lease = clientv3.NewLease(wr.client)

	// 1. Grant new lease
	leaseResp, err := wr.lease.Grant(wr.ctx, wr.ttl)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	// 2. Prepare node information
	nodeKey := wr.getNodeKey()
	nodeInfoBytes, err := json.Marshal(wr.nodeInfo)
	if err != nil {
		wr.lease.Revoke(context.Background(), leaseResp.ID)
		return fmt.Errorf("failed to marshal node info: %w", err)
	}

	// 3. Use transaction to ensure node ID uniqueness
	// Check if key exists and if it does, verify if it has the same lease
	getResp, err := wr.client.Get(wr.ctx, nodeKey)
	if err != nil {
		wr.lease.Revoke(context.Background(), leaseResp.ID)
		return fmt.Errorf("failed to check existing node: %w", err)
	}

	txn := wr.client.Txn(wr.ctx)
	var txnResp *clientv3.TxnResponse

	if len(getResp.Kvs) == 0 {
		// Key doesn't exist, try to create it
		txnResp, err = txn.If(
			clientv3.Compare(clientv3.CreateRevision(nodeKey), "=", 0),
		).Then(
			clientv3.OpPut(nodeKey, string(nodeInfoBytes), clientv3.WithLease(leaseResp.ID)),
		).Else(
			clientv3.OpGet(nodeKey),
		).Commit()
	} else {
		// Key exists, check if it has a lease (if no lease, the previous node died ungracefully)
		existingLease := getResp.Kvs[0].Lease
		if existingLease == 0 || existingLease == int64(wr.leaseID) {
			// No lease or same lease (re-registration), we can take over
			txnResp, err = txn.Then(
				clientv3.OpPut(nodeKey, string(nodeInfoBytes), clientv3.WithLease(leaseResp.ID)),
			).Commit()
		} else {
			// Different lease exists, another node is active
			wr.lease.Revoke(context.Background(), leaseResp.ID)
			return fmt.Errorf("node already exists")
		}
	}

	if err != nil {
		// Revoke lease if transaction failed
		wr.lease.Revoke(context.Background(), leaseResp.ID)
		return fmt.Errorf("failed to register node in etcd: %w", err)
	}

	if !txnResp.Succeeded {
		// Transaction failed, another node holds the key
		wr.lease.Revoke(context.Background(), leaseResp.ID)
		return fmt.Errorf("node already exists")
	}

	// Success - update lease ID
	wr.leaseID = leaseResp.ID
	return nil
}

// UnregisterNode removes the writer node from etcd
func (wr *WriterRegistry) UnregisterNode() error {
	// Cancel context to stop keep-alive goroutine
	wr.cancel()

	// Revoke lease, which will automatically delete the key
	_, err := wr.lease.Revoke(context.Background(), wr.leaseID)
	if err != nil {
		log.Printf("[Writer Registry] Failed to revoke lease: %v", err)
	}

	log.Printf("[Writer Registry] Node %s unregistered from chain %s", wr.nodeID, wr.chainID)
	wr.lease.Close()
	return err
}

// startKeepAlive starts a goroutine to keep the lease alive using KeepAliveOnce
func (wr *WriterRegistry) startKeepAlive() {
	// Calculate keep-alive interval as TTL/4 for safety margin
	keepAliveInterval := time.Duration(wr.ttl/4) * time.Second
	if keepAliveInterval < 1*time.Second {
		keepAliveInterval = 1 * time.Second
	}

	const maxFail = 3 // Allow 3 consecutive failures before rebuilding
	failCount := 0

	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()

	log.Printf("[Writer Registry] Started keep-alive for node %s (interval: %v)", wr.nodeID, keepAliveInterval)

	for {
		select {
		case <-wr.ctx.Done():
			log.Printf("[Writer Registry] Keep-alive stopped for node %s", wr.nodeID)
			return

		case <-ticker.C:
			ctx, cancel := context.WithTimeout(wr.ctx, 3*time.Second)
			resp, err := wr.lease.KeepAliveOnce(ctx, wr.leaseID)
			cancel()

			if err != nil || (resp != nil && resp.TTL <= 0) {
				failCount++
				if err != nil {
					log.Printf("[Writer Registry] Failed to renew lease for node %s (%d/%d): %v", wr.nodeID, failCount, maxFail, err)
				} else {
					log.Printf("[Writer Registry] WARNING: Lease expired for node %s (%d/%d, TTL=%d)", wr.nodeID, failCount, maxFail, resp.TTL)
				}

				if failCount >= maxFail {
					log.Printf("[Writer Registry] Max retries reached for node %s, attempting to rebuild lease", wr.nodeID)
					if err := wr.rebuildLease(); err != nil {
						log.Printf("[Writer Registry] Failed to rebuild lease for node %s: %v, stopping keep-alive", wr.nodeID, err)
						return
					}
					failCount = 0 // Reset counter after successful rebuild
					log.Printf("[Writer Registry] Lease rebuilt successfully for node %s, continuing keep-alive", wr.nodeID)
				}
			} else {
				// Success
				if failCount > 0 {
					log.Printf("[Writer Registry] Lease renewal recovered for node %s after %d failures", wr.nodeID, failCount)
				}
				failCount = 0
			}
		}
	}
}

// rebuildLease rebuilds the lease when keepalive fails repeatedly
func (wr *WriterRegistry) rebuildLease() error {
	const (
		maxRetries    = 5
		retryInterval = 5 * time.Second
	)

	log.Printf("[Writer Registry] Rebuilding lease for node %s", wr.nodeID)

	// 2. Retry loop for granting lease and registering key
	var lastErr error
	for retry := 0; retry < maxRetries; retry++ {
		// Check if context is cancelled
		select {
		case <-wr.ctx.Done():
			return fmt.Errorf("context cancelled during lease rebuild: %w", wr.ctx.Err())
		default:
		}

		// Use the same transaction-based registration logic
		err := wr.grantLeaseAndRegisterKey()
		if err != nil {
			lastErr = err
			log.Printf("[Writer Registry] Rebuild attempt %d/%d failed for node %s: %v", retry+1, maxRetries, wr.nodeID, err)
			time.Sleep(retryInterval)
			continue
		}

		// Success
		log.Printf("[Writer Registry] Lease rebuilt successfully for node %s (new leaseID: %x, attempts: %d)", wr.nodeID, wr.leaseID, retry+1)
		return nil
	}

	// All retries failed
	return fmt.Errorf("failed to rebuild lease after %d attempts: %w", maxRetries, lastErr)
}

func (wr *WriterRegistry) getNodeKey() string {
	key := wr.chainID
	if wr.version != "" {
		key += "/" + wr.version
	}
	return fmt.Sprintf("%s/writers/%s", key, wr.nodeID)
}
