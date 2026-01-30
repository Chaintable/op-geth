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
	lease := clientv3.NewLease(wr.client)
	leaseResp, err := lease.Grant(wr.ctx, wr.ttl)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}
	wr.leaseID = leaseResp.ID
	wr.lease = lease

	// Register node information
	nodeKey := wr.getNodeKey()
	nodeInfoBytes, err := json.Marshal(wr.nodeInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}

	// Use transaction to ensure node ID uniqueness
	// Check if key exists and if it does, verify if it has the same lease
	getResp, err := wr.client.Get(wr.ctx, nodeKey)
	if err != nil {
		wr.lease.Revoke(context.Background(), wr.leaseID)
		return fmt.Errorf("failed to check existing node: %w", err)
	}

	txn := wr.client.Txn(wr.ctx)
	var txnResp *clientv3.TxnResponse

	if len(getResp.Kvs) == 0 {
		// Key doesn't exist, try to create it
		txnResp, err = txn.If(
			clientv3.Compare(clientv3.CreateRevision(nodeKey), "=", 0),
		).Then(
			clientv3.OpPut(nodeKey, string(nodeInfoBytes), clientv3.WithLease(wr.leaseID)),
		).Else(
			clientv3.OpGet(nodeKey),
		).Commit()
	} else {
		// Key exists, check if it has a lease (if no lease, the previous node died ungracefully)
		existingLease := getResp.Kvs[0].Lease
		if existingLease == 0 || existingLease == int64(wr.leaseID) {
			// No lease or same lease (re-registration), we can take over
			txnResp, err = txn.Then(
				clientv3.OpPut(nodeKey, string(nodeInfoBytes), clientv3.WithLease(wr.leaseID)),
			).Commit()
		} else {
			// Different lease exists, another node is active
			// Create a fake failed transaction response
			txnResp = &clientv3.TxnResponse{
				Succeeded: false,
			}
			// We'll handle the error message below using getResp.Kvs
			err = nil // Clear error as we want to handle it as a failed transaction
		}
	}

	if err != nil {
		// Revoke lease if transaction failed
		wr.lease.Revoke(context.Background(), wr.leaseID)
		return fmt.Errorf("failed to register node in etcd: %w", err)
	}

	if !txnResp.Succeeded {
		// Node with same ID already exists, revoke our lease and panic
		wr.lease.Revoke(context.Background(), wr.leaseID)

		// Get existing node info for error message
		existingNode := ""
		// Try to get from transaction response first
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				existingNode = string(rangeResp.Kvs[0].Value)
			}
		}
		// If not in transaction response, use the initial get response
		if existingNode == "" && len(getResp.Kvs) > 0 {
			existingNode = string(getResp.Kvs[0].Value)
		}

		// Panic to prevent duplicate nodes from running
		panic(fmt.Sprintf("[Writer Registry] Node with ID %s already exists for chain %s. Existing node info: %s",
			wr.nodeID, wr.chainID, existingNode))
	}

	// Keep lease alive using KeepAliveOnce with retry
	go wr.startKeepAlive()

	log.Printf("[Writer Registry] Node %s registered successfully for chain %s with lease %d",
		wr.nodeID, wr.chainID, wr.leaseID)
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

			if err != nil {
				log.Printf("[Writer Registry] Failed to renew lease for node %s: %v", wr.nodeID, err)
			} else if resp.TTL <= 0 {
				log.Printf("[Writer Registry] WARNING: Lease expired for node %s (TTL=%d)", wr.nodeID, resp.TTL)
			}
		}
	}
}

func (wr *WriterRegistry) getNodeKey() string {
	key := wr.chainID
	if wr.version != "" {
		key += "/" + wr.version
	}
	return fmt.Sprintf("%s/writers/%s", key, wr.nodeID)
}
