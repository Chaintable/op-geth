package miner

import (
	"fmt"
	"time"

	ptracer "github.com/Chaintable/pipeline/tracer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
)

// generateTracedPayload uses the exact same block processor as import. In
// particular, fork transitions, system calls, L1 fee accounting, transaction
// callbacks and consensus Finalize all run once and in the same order.
func (w *worker) generateTracedPayload(work *environment, args *generateParams) *newPayloadResult {
	if !args.noTxs || work.header.Difficulty.Sign() != 0 {
		return &newPayloadResult{err: fmt.Errorf("payload execution reuse requires a fixed OP PoS block")}
	}
	configHash := w.chain.PayloadExecutionConfigHash()
	header := types.CopyHeader(work.header)
	withdrawals := args.withdrawals
	// Match Beacon.FinalizeAndAssemble's normalization of an empty list.
	if w.chainConfig.IsShanghai(header.Number, header.Time) && withdrawals == nil {
		withdrawals = make(types.Withdrawals, 0)
	}
	// Roots, gas used and the block hash are provisional; none is an EVM input.
	// Receipt/log block hashes are rebound after the complete block is assembled.
	candidate := types.NewBlockWithWithdrawals(header, args.txs, nil, nil, withdrawals, trie.NewStackTrie(nil))
	trace := ptracer.NewPayloadTracer(header)
	vmConfig := *w.chain.GetVMConfig()
	vmConfig.ExtraEips = append([]int(nil), vmConfig.ExtraEips...)
	vmConfig.Tracer = trace
	processor := core.NewStateProcessor(w.chainConfig, w.chain, w.engine)
	start := time.Now()
	receipts, logs, usedGas, err := processor.Process(candidate, work.state, vmConfig)
	// A cached StateDB must not retain a build collector through callback closures.
	work.state.SetLiveTraceHooks(nil, nil)
	if err != nil {
		return &newPayloadResult{err: err}
	}
	commitDepositTxsTimer.UpdateSince(start)

	start = time.Now()
	header.GasUsed = usedGas
	header.Root = work.state.IntermediateRoot(w.chainConfig.IsEIP158(header.Number))
	if err := work.state.Error(); err != nil {
		return &newPayloadResult{err: err}
	}
	if header.Root == (common.Hash{}) {
		return &newPayloadResult{err: fmt.Errorf("empty payload state root")}
	}
	// Process has already called Finalize, including withdrawals. Do not call
	// FinalizeAndAssemble here, which would apply those state changes twice.
	block := types.NewBlockWithWithdrawals(header, args.txs, nil, receipts, withdrawals, trie.NewStackTrie(nil))
	for i, receipt := range receipts {
		receipt.BlockHash = block.Hash()
		receipt.BlockNumber = block.Number()
		receipt.TransactionIndex = uint(i)
		for _, entry := range receipt.Logs {
			entry.BlockHash = block.Hash()
			entry.BlockNumber = block.NumberU64()
		}
	}
	if err := trace.SealPayload(block); err != nil {
		return &newPayloadResult{err: err}
	}
	assembleBlockTimer.UpdateSince(start)
	return &newPayloadResult{
		block: block,
		fees:  totalFees(block, receipts),
		execution: &core.PayloadExecution{
			Block:           block,
			ParentStateRoot: work.state.OriginalRoot(),
			ConfigHash:      configHash,
			State:           work.state,
			Receipts:        receipts,
			Logs:            logs,
			UsedGas:         usedGas,
			Trace:           trace,
		},
	}
}
