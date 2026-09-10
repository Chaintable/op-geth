package core

import (
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ptracer "github.com/Chaintable/pipeline/tracer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

func cacheTestPayload(id byte) *PayloadExecution {
	return &PayloadExecution{Block: types.NewBlockWithHeader(&types.Header{
		Number: big.NewInt(1), Difficulty: big.NewInt(0), Extra: []byte{id},
	})}
}

func TestPayloadExecutionCacheLimitsAndOwnership(t *testing.T) {
	cache := newPayloadExecutionCache(ptracer.PayloadCacheConfig{MaxEntries: 2, MaxBytes: 100, TTLSeconds: 30})
	defer cache.close()
	first, second := cacheTestPayload(1), cacheTestPayload(2)
	if !cache.put(first, 60) || !cache.put(second, 60) {
		t.Fatal("failed to admit an eligible candidate")
	}
	if cache.take(first.Block.Hash()) != nil {
		t.Fatal("memory budget did not evict the oldest candidate")
	}
	var consumers atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cache.take(second.Block.Hash()) != nil {
				consumers.Add(1)
			}
		}()
	}
	wg.Wait()
	if consumers.Load() != 1 || cache.bytes != 0 {
		t.Fatal("a candidate was not consumed exactly once")
	}
	if cache.put(first, 101) {
		t.Fatal("accepted a candidate larger than the byte budget")
	}
	cache.put(first, 20)
	cache.put(second, 20)
	third := cacheTestPayload(3)
	cache.put(third, 20)
	if cache.take(first.Block.Hash()) != nil {
		t.Fatal("entry limit did not evict the oldest candidate")
	}
	cache.close()
	if cache.bytes != 0 || len(cache.entries) != 0 || cache.put(first, 20) {
		t.Fatal("closed cache retained or accepted a candidate")
	}
}

func TestPayloadExecutionCacheExpiration(t *testing.T) {
	cache := newPayloadExecutionCache(ptracer.PayloadCacheConfig{MaxEntries: 2, MaxBytes: 100, TTLSeconds: 30})
	defer cache.close()
	entry := cacheTestPayload(1)
	cache.put(entry, 20)
	cache.mu.Lock()
	cache.entries[entry.Block.Hash()].created = time.Now().Add(-time.Minute)
	cache.mu.Unlock()
	if cache.take(entry.Block.Hash()) != nil || cache.bytes != 0 {
		t.Fatal("consumed an expired candidate")
	}
}

func TestPayloadExecutionRejectsDifferentContext(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabase(db), nil)
	if err != nil {
		t.Fatal(err)
	}
	block := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1), Difficulty: big.NewInt(0), Root: types.EmptyRootHash})
	trace := ptracer.NewPayloadTracer(block.Header())
	if err := trace.SealPayload(block); err != nil {
		t.Fatal(err)
	}
	execution := &PayloadExecution{Block: block, ParentStateRoot: types.EmptyRootHash, State: statedb, Trace: trace}
	if !execution.matches(block, types.EmptyRootHash) {
		t.Fatal("rejected a complete empty-block execution")
	}
	if execution.matches(block, common.HexToHash("0x01")) {
		t.Fatal("accepted a different parent state")
	}
	different := types.CopyHeader(block.Header())
	different.Time++
	if execution.matches(types.NewBlockWithHeader(different), types.EmptyRootHash) {
		t.Fatal("accepted another block at the same height")
	}
	execution.UsedGas = 1
	if execution.matches(block, types.EmptyRootHash) {
		t.Fatal("trusted header gas instead of locally measured gas")
	}
	execution.UsedGas = 0
	execution.Receipts = []*types.Receipt{{}}
	if execution.matches(block, types.EmptyRootHash) {
		t.Fatal("accepted inconsistent receipt data")
	}
}

func TestPayloadExecutionConfigChanges(t *testing.T) {
	bc := &BlockChain{chainConfig: &params.ChainConfig{ChainID: big.NewInt(204)}}
	initial := bc.PayloadExecutionConfigHash()
	if initial == (common.Hash{}) {
		t.Fatal("could not fingerprint the execution configuration")
	}
	bc.chainConfig.CanyonTime = new(uint64)
	if initial == bc.PayloadExecutionConfigHash() {
		t.Fatal("fork changes did not invalidate the execution configuration")
	}
	initial = bc.PayloadExecutionConfigHash()
	bc.vmConfig.EnableOpcodeOptimizations = true
	if initial == bc.PayloadExecutionConfigHash() {
		t.Fatal("VM changes did not invalidate the execution configuration")
	}
}
