// Copyright 2022 The go-ethereum Authors
// // This file is part of the go-ethereum library.
// //
// // The go-ethereum library is free software: you can redistribute it and/or modify
// // it under the terms of the GNU Lesser General Public License as published by
// // the Free Software Foundation, either version 3 of the License, or
// // (at your option) any later version.
// //
// // The go-ethereum library is distributed in the hope that it will be useful,
// // but WITHOUT ANY WARRANTY; without even the implied warranty of
// // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// // GNU Lesser General Public License for more details.
// //
// // You should have received a copy of the GNU Lesser General Public License
// // along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
//
// // Package ethapi implements the general Ethereum API functions.

package eth

import (
	"bytes"
	"context"
	"fmt"
	txtracelib "github.com/DeBankDeFi/etherlib/pkg/txtracev2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/debank/tracer"
	ptracer "github.com/ethereum/go-ethereum/debank/tracer"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/debank/util"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"strings"
)

// PublicTxTraceAPI provides an API to tracing transaction or block information.
// // It offers only methods that operate on public data that is freely available to anyone.
type PublicTxTraceAPI struct {
	e *Ethereum
}

// NewPublicTxTraceAPI creates a new trace API.
func NewPublicTxTraceAPI(e *Ethereum) *PublicTxTraceAPI {
	return &PublicTxTraceAPI{e: e}
}

// Transaction trace_transaction function returns transaction traces.
func (api *PublicTxTraceAPI) Transaction(ctx context.Context, txHash common.Hash) (interface{}, error) {
	if api.e.blockchain == nil {
		return []byte{}, fmt.Errorf("blockchain corruput")
	}

	raw, err := api.e.blockchain.TxTraceStore().ReadTxTrace(ctx, txHash)
	if err != nil {
		return []byte{}, err
	}

	if bytes.Equal(raw, []byte{}) { // empty response
		return nil, fmt.Errorf("trace result of tx {%#v} not found in tracedb", txHash)
	}

	flatten := new(txtracelib.ActionTraceList)
	err = rlp.DecodeBytes(raw, flatten)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rlp flatten traces: %v", err)
	}

	return *flatten, nil
}

func (api *PublicTxTraceAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ptypes.DebankOutPut, error) {
	block, err := api.e.APIBackend.BlockByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if block.NumberU64() == 0 {
		genesis, err := core.ReadGenesis(api.e.chainDb)
		if err != nil {
			return nil, fmt.Errorf("could not read genesis: %w", err)
		}
		header := util.BuildPilelineBlockHeader(block)
		blockDiff := ptracer.GenesisAllocToStateDiff(genesis.Alloc)
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
			StateDiff:      stateDiffBytes,
			ValidationHash: blockFile.Validation().ValidationHash,
		}, nil
	}
	// Prepare base state
	parent, err := api.e.APIBackend.BlockByHash(ctx, block.ParentHash())
	if err != nil {
		return nil, err
	}
	statedb, release, err := api.e.APIBackend.StateAtBlock(ctx, parent, 128, nil, true, false)
	if err != nil {
		return nil, err
	}
	defer release()

	rpcTracer := tracer.NewLocalTracer()

	blockCtx := core.NewEVMBlockContext(block.Header(), api.e.blockchain, nil, api.e.APIBackend.ChainConfig(), statedb)

	rpcTracer.OnBlockStart(block)

	var (
		txs = block.Transactions()
		gp  = new(core.GasPool).AddGas(block.GasLimit())
	)

	cfg := vm.Config{
		Debug:   true,
		PreExec: true,
		Tracer:  rpcTracer,
	}

	for i, tx := range txs {
		evm := vm.NewEVM(blockCtx, vm.TxContext{}, statedb, api.e.APIBackend.ChainConfig(), cfg)

		msg, err := core.TransactionToMessage(tx, types.MakeSigner(api.e.APIBackend.ChainConfig(), block.Number()), blockCtx.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		statedb.SetTxContext(tx.Hash(), i)

		_, err = core.ApplyMessage(evm, msg, gp)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		//receipt.SetEffectiveGasPrice(tx, blockCtx.BaseFee)
	}

	root, destructs, accounts, storages, codes, err := statedb.StateDiff(api.e.APIBackend.ChainConfig().IsEIP158(block.Number()))
	if err != nil {
		return nil, fmt.Errorf("could not get state diff: %w", err)
	}

	if root != block.Header().Root {
		return nil, fmt.Errorf("state root mismatch: expected %x, got %x", block.Header().Root, root)
	}

	parentRoot := parent.Root()

	res := rpcTracer.OutPut(parentRoot, root, destructs, accounts, storages, codes)

	return res, nil
}
