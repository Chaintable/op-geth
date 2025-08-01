package eth

import (
	"context"
	"fmt"
	"strings"

	ptracer "github.com/Chaintable/pipeline/tracer"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type DebankAPI struct {
	eth *Ethereum
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	return &DebankAPI{
		eth: eth,
	}
}

func (api *DebankAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ptypes.DebankOutPut, error) {
	block, err := api.eth.APIBackend.BlockByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if block.NumberU64() == 0 {
		genesis, err := core.ReadGenesis(api.eth.chainDb)
		if err != nil {
			return nil, fmt.Errorf("could not read genesis: %w", err)
		}
		header := util.BuildPilelineBlockHeader(block)
		blockDiff := ptracer.GenesisAllocToStateDiff(genesis.Alloc)
		blockDiff.Hash = header.StateRoot
		blockFile := &ptypes.BlockFile{
			Block:            util.BuildPipelineBlock(block),
			Txs:              make([]ptypes.Transaction, 0),
			Events:           make([]ptypes.Event, 0),
			Traces:           make([]ptypes.Trace, 0),
			ErrorEvents:      make([]ptypes.Event, 0),
			ErrorTraces:      make([]ptypes.Trace, 0),
			StorageContracts: make([]string, 0),
		}
		for addr, account := range genesis.Alloc {
			if len(account.Storage) > 0 {
				blockFile.StorageContracts = append(blockFile.StorageContracts, strings.ToLower(addr.Hex()))
			}
		}
		var stateDiffBytes []byte
		if blockDiff != nil {
			stateDiffBytes, err = util.EncodeToRlp(blockDiff)
			if err != nil {
				log.Error("Failed to encode state diff", "err", err)
				stateDiffBytes = []byte{}
			}
		} else {
			stateDiffBytes = []byte{}
		}

		return &ptypes.DebankOutPut{
			BlockFile:      blockFile,
			Header:         header,
			StateDiff:      hexutil.Bytes(stateDiffBytes),
			ValidationHash: blockFile.Validation().ValidationHash,
		}, nil
	}
	// Prepare base state
	parent, err := api.eth.APIBackend.BlockByHash(ctx, block.ParentHash())
	if err != nil {
		return nil, err
	}
	statedb, release, err := api.eth.APIBackend.StateAtBlock(ctx, parent, 128, nil, true, false)
	if err != nil {
		return nil, err
	}
	defer release()

	rpcTracer := ptracer.RPCTracer{}
	tracer := &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: rpcTracer.OnTxStart,
			OnTxEnd:   rpcTracer.OnTxEnd,
			OnEnter:   rpcTracer.OnEnter,
			OnExit:    rpcTracer.OnExit,
			OnOpcode:  rpcTracer.OnOpcode,
			OnLog:     rpcTracer.OnLog,
		},
		Stop:      rpcTracer.Stop,
		GetResult: rpcTracer.GetResult,
	}
	tracingStateDB := state.NewHookedState(statedb, tracer.Hooks)
	blockCtx := core.NewEVMBlockContext(block.Header(), ethapi.NewChainContext(ctx, api.eth.APIBackend), nil, api.eth.APIBackend.ChainConfig(), statedb)
	evm := vm.NewEVM(blockCtx, tracingStateDB, api.eth.APIBackend.ChainConfig(), vm.Config{Tracer: tracer.Hooks})

	rpcTracer.OnBlockStart(block)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		core.ProcessBeaconBlockRoot(*beaconRoot, evm)
	}
	if api.eth.APIBackend.ChainConfig().IsPrague(block.Number(), block.Time()) || api.eth.APIBackend.ChainConfig().IsVerkle(block.Number(), block.Time()) {
		core.ProcessParentBlockHash(block.ParentHash(), evm)
	}
	var (
		txs     = block.Transactions()
		signer  = types.MakeSigner(api.eth.APIBackend.ChainConfig(), block.Number(), block.Time())
		gp      = new(core.GasPool).AddGas(block.GasLimit())
		usedGas = new(uint64)
	)

	for i, tx := range txs {
		msg, err := core.TransactionToMessage(tx, signer, blockCtx.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		statedb.SetTxContext(tx.Hash(), i)

		receipt, err := core.ApplyTransactionWithEVM(msg, gp, statedb, block.Number(), block.Hash(), tx, usedGas, evm)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		receipt.SetEffectiveGasPrice(tx, blockCtx.BaseFee)
	}

	root, destructs, accounts, storages, codes, err := statedb.StateDiff(api.eth.APIBackend.ChainConfig().IsEIP158(block.Number()))
	if err != nil {
		return nil, fmt.Errorf("could not get state diff: %w", err)
	}

	if root != block.Header().Root {
		return nil, fmt.Errorf("state root mismatch: expected %x, got %x", block.Header().Root, root)
	}

	parentRoot := parent.Root()

	res := rpcTracer.GetOutPut(parentRoot, root, destructs, accounts, storages, codes)

	return res, nil
}
