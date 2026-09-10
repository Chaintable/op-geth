package core

import (
	"encoding/json"
	"sync"
	"time"

	ptracer "github.com/Chaintable/pipeline/tracer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
)

var (
	payloadExecutionHits     = metrics.NewRegisteredCounter("chain/payload/execution/hit", nil)
	payloadExecutionMisses   = metrics.NewRegisteredCounter("chain/payload/execution/miss", nil)
	payloadExecutionRejected = metrics.NewRegisteredCounter("chain/payload/execution/rejected", nil)
)

// PayloadExecution owns one fully executed candidate. Ownership moves from the
// builder into the cache and then, at most once, into the importer. No StateDB
// copy is involved; the block's original values and destruction markers survive.
type PayloadExecution struct {
	Block           *types.Block
	ParentStateRoot common.Hash
	ConfigHash      common.Hash
	State           *state.StateDB
	Receipts        types.Receipts
	Logs            []*types.Log
	UsedGas         uint64
	Trace           *ptracer.PipelineTracer
}

type payloadExecutionEntry struct {
	execution *PayloadExecution
	bytes     uint64
	created   time.Time
	timer     *time.Timer
}

type payloadExecutionCache struct {
	mu      sync.Mutex
	entries map[common.Hash]*payloadExecutionEntry
	bytes   uint64
	limits  ptracer.PayloadCacheConfig
	closed  bool
}

func newPayloadExecutionCache(limits ptracer.PayloadCacheConfig) *payloadExecutionCache {
	return &payloadExecutionCache{entries: make(map[common.Hash]*payloadExecutionEntry), limits: limits}
}

func (c *payloadExecutionCache) remove(hash common.Hash) *PayloadExecution {
	e := c.entries[hash]
	if e == nil {
		return nil
	}
	delete(c.entries, hash)
	c.bytes -= e.bytes
	if e.timer != nil {
		e.timer.Stop()
	}
	return e.execution
}

func (c *payloadExecutionCache) put(execution *PayloadExecution, size uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.limits.MaxEntries <= 0 || c.limits.TTLSeconds <= 0 || size > c.limits.MaxBytes {
		return false
	}
	hash := execution.Block.Hash()
	c.remove(hash)
	for len(c.entries) >= c.limits.MaxEntries || c.bytes > c.limits.MaxBytes-size {
		var oldestHash common.Hash
		var oldest *payloadExecutionEntry
		for h, entry := range c.entries {
			if oldest == nil || entry.created.Before(oldest.created) {
				oldestHash, oldest = h, entry
			}
		}
		c.remove(oldestHash)
	}
	e := &payloadExecutionEntry{execution: execution, bytes: size, created: time.Now()}
	c.entries[hash] = e
	c.bytes += size
	// Release expired candidates even if the node stops receiving Engine requests.
	e.timer = time.AfterFunc(time.Duration(c.limits.TTLSeconds)*time.Second, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.entries[hash] == e {
			c.remove(hash)
		}
	})
	return true
}

func (c *payloadExecutionCache) take(hash common.Hash) *PayloadExecution {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[hash]
	if e == nil {
		return nil
	}
	execution := c.remove(hash)
	if time.Since(e.created) >= time.Duration(c.limits.TTLSeconds)*time.Second {
		return nil
	}
	return execution
}

func (c *payloadExecutionCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for hash := range c.entries {
		c.remove(hash)
	}
}

// PayloadTraceCacheEnabled deliberately excludes custom processors, PoW engines,
// precompile overrides and TxDAG. The initial implementation handles OP PoS
// blocks executed by the same StateProcessor in both phases.
func (bc *BlockChain) PayloadTraceCacheEnabled() bool {
	_, beaconEngine := bc.engine.(*beacon.Beacon)
	_, standardProcessor := bc.processor.(*StateProcessor)
	return bc.payloadExecutions != nil && bc.pipelineTracer != nil &&
		bc.vmConfig.Tracer == bc.pipelineTracer && bc.chainConfig.Optimism != nil &&
		beaconEngine && standardProcessor && !bc.enableTxDAG &&
		bc.cacheConfig != nil && !bc.cacheConfig.NoTries &&
		!bc.vmConfig.NoBaseFee && bc.vmConfig.OptimismPrecompileOverrides == nil
}

// PayloadExecutionConfigHash records the chain rules and execution options at
// build time. A changed configuration turns a later lookup into a cache miss.
func (bc *BlockChain) PayloadExecutionConfigHash() common.Hash {
	encoded, err := json.Marshal(struct {
		Chain               *params.ChainConfig
		Preimages           bool
		ExtraEips           []int
		OpcodeOptimizations bool
	}{bc.chainConfig, bc.vmConfig.EnablePreimageRecording, bc.vmConfig.ExtraEips, bc.vmConfig.EnableOpcodeOptimizations})
	if err != nil {
		return common.Hash{}
	}
	return crypto.Keccak256Hash(encoded)
}

// CachePayloadExecution takes ownership only after the builder has stopped all
// state access, including its prefetcher. It never inserts a block into chain DB
// or the known-block cache, which would bypass newPayload validation.
func (bc *BlockChain) CachePayloadExecution(e *PayloadExecution) bool {
	if !bc.PayloadTraceCacheEnabled() || e == nil || e.Block == nil ||
		e.ConfigHash == (common.Hash{}) || e.ConfigHash != bc.PayloadExecutionConfigHash() ||
		!e.matches(e.Block, e.ParentStateRoot) {
		return false
	}
	size := e.State.EstimatedMemorySize() + e.Trace.EstimatedPayloadSize() + uint64(e.Block.Size())
	// StateDB owns the log bytes, but account for receipt objects and their slices.
	size += uint64(len(e.Receipts))*1024 + uint64(len(e.Logs))*128
	if !bc.payloadExecutions.put(e, size) {
		payloadExecutionRejected.Inc(1)
		log.Debug("Payload execution exceeds cache limits", "hash", e.Block.Hash(), "bytes", size)
		return false
	}
	log.Debug("Cached payload execution", "number", e.Block.NumberU64(), "hash", e.Block.Hash(), "bytes", size)
	return true
}

func (e *PayloadExecution) matches(block *types.Block, parentRoot common.Hash) bool {
	if e == nil || e.Block == nil || e.State == nil || e.State.Error() != nil ||
		e.Block.Hash() != block.Hash() || e.ParentStateRoot != parentRoot ||
		e.State.OriginalRoot() != parentRoot || e.UsedGas != block.GasUsed() ||
		len(e.Receipts) != len(block.Transactions()) || !e.Trace.MatchesPayload(block) {
		return false
	}
	var gas uint64
	var logs int
	for i, receipt := range e.Receipts {
		if receipt == nil || receipt.TxHash != block.Transactions()[i].Hash() ||
			receipt.BlockHash != block.Hash() || receipt.TransactionIndex != uint(i) ||
			receipt.BlockNumber == nil || receipt.BlockNumber.Cmp(block.Number()) != 0 ||
			receipt.GasUsed > e.UsedGas-gas {
			return false
		}
		gas += receipt.GasUsed
		if gas != receipt.CumulativeGasUsed {
			return false
		}
		for _, entry := range receipt.Logs {
			if entry == nil || logs >= len(e.Logs) || e.Logs[logs] != entry || entry.BlockHash != block.Hash() ||
				entry.BlockNumber != block.NumberU64() || entry.TxHash != receipt.TxHash || entry.TxIndex != uint(i) {
				return false
			}
			logs++
		}
	}
	return gas == e.UsedGas && logs == len(e.Logs)
}

func (bc *BlockChain) takePayloadExecution(block *types.Block, parentRoot common.Hash) *PayloadExecution {
	if !bc.PayloadTraceCacheEnabled() {
		return nil
	}
	e := bc.payloadExecutions.take(block.Hash())
	if e == nil {
		payloadExecutionMisses.Inc(1)
		return nil
	}
	if e.ConfigHash != bc.PayloadExecutionConfigHash() || !e.matches(block, parentRoot) {
		payloadExecutionMisses.Inc(1)
		log.Debug("Discarding mismatched payload execution", "hash", block.Hash(), "parentRoot", parentRoot)
		return nil
	}
	return e
}
