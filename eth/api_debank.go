package eth

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"

	ptracer "github.com/Chaintable/pipeline/tracer"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers"
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

		// 构造 genesis tx 和 trace
		zeroAddr := "0x0000000000000000000000000000000000000000"
		txIdx := int64(0)

		// 对地址排序，确保遍历顺序确定性
		sortedAddrs := make([]common.Address, 0, len(genesis.Alloc))
		for addr := range genesis.Alloc {
			sortedAddrs = append(sortedAddrs, addr)
		}
		sort.Slice(sortedAddrs, func(i, j int) bool {
			return sortedAddrs[i].Hex() < sortedAddrs[j].Hex()
		})

		for _, addr := range sortedAddrs {
			account := genesis.Alloc[addr]
			addrLower := strings.ToLower(addr.Hex())

			// 处理有 Storage 的账户
			if len(account.Storage) > 0 {
				blockFile.StorageContracts = append(blockFile.StorageContracts, addrLower)
			}

			// 处理有 balance 的账户 - 构造转账 tx 和 call trace
			if account.Balance != nil && account.Balance.Sign() > 0 {
				// tx id: 0xgenesis01 + 13个0 + 地址(42字符) = 66字符
				txID := fmt.Sprintf("0xgenesis01%013d%s", 0, addrLower)

				tx := ptypes.Transaction{
					ID:               txID,
					From:             zeroAddr,
					To:               addrLower,
					Gas:              big.NewInt(0),
					GasPrice:         big.NewInt(0),
					GasUsed:          big.NewInt(0),
					Status:           true,
					GasFeeCap:        big.NewInt(0),
					GasTipCap:        big.NewInt(0),
					Input:            []byte{},
					Nonce:            big.NewInt(0),
					TransactionIndex: txIdx,
					Value:            (*hexutil.Big)(account.Balance),
				}
				blockFile.Txs = append(blockFile.Txs, tx)

				// trace id = hash(tx_id, parent_trace_id, pos_in_parent_trace)
				traceID := util.ToHash([]string{txID, "", "0"})
				trace := ptypes.Trace{
					ID:                traceID,
					From:              zeroAddr,
					Gas:               big.NewInt(0),
					Input:             []byte{},
					To:                addrLower,
					Value:             (*hexutil.Big)(account.Balance),
					GasUsed:           big.NewInt(0),
					Output:            []byte{},
					CallCreateType:    "call",
					CallType:          "call",
					TxID:              txID,
					ParentTraceID:     "",
					PosInParentTrace:  0,
					SelfStorageChange: false,
					StorageChange:     false,
					Subtraces:         0,
					TraceAddress:      []int64{},
				}
				blockFile.Traces = append(blockFile.Traces, trace)
				txIdx++
			}

			// 处理有 code 的账户 - 构造 create tx 和 create trace
			if len(account.Code) > 0 {
				// tx id: 0xgenesis02 + 13个0 + 地址(42字符) = 66字符
				txID := fmt.Sprintf("0xgenesis02%013d%s", 0, addrLower)

				tx := ptypes.Transaction{
					ID:               txID,
					From:             zeroAddr,
					To:               addrLower,
					Gas:              big.NewInt(0),
					GasPrice:         big.NewInt(0),
					GasUsed:          big.NewInt(0),
					Status:           true,
					GasFeeCap:        big.NewInt(0),
					GasTipCap:        big.NewInt(0),
					Input:            account.Code,
					Nonce:            big.NewInt(0),
					TransactionIndex: txIdx,
					Value:            (*hexutil.Big)(big.NewInt(0)),
				}
				blockFile.Txs = append(blockFile.Txs, tx)

				// trace id = hash(tx_id, parent_trace_id, pos_in_parent_trace)
				traceID := util.ToHash([]string{txID, "", "0"})
				trace := ptypes.Trace{
					ID:                traceID,
					From:              zeroAddr,
					Gas:               big.NewInt(0),
					Input:             account.Code,
					To:                addrLower,
					Value:             (*hexutil.Big)(big.NewInt(0)),
					GasUsed:           big.NewInt(0),
					Output:            account.Code, // output 直接使用 input (code)
					CallCreateType:    "create",
					CallType:          "",
					TxID:              txID,
					ParentTraceID:     "",
					PosInParentTrace:  0,
					SelfStorageChange: false,
					StorageChange:     false,
					Subtraces:         0,
					TraceAddress:      []int64{},
				}
				blockFile.Traces = append(blockFile.Traces, trace)
				txIdx++
			}
		}

		// 添加原生代币合约创建 tx 和 trace (E地址)
		nativeTokenAddr := "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		nativeTokenTxID := fmt.Sprintf("0xgenesis03%013d%s", 0, nativeTokenAddr)

		nativeTokenTx := ptypes.Transaction{
			ID:               nativeTokenTxID,
			From:             zeroAddr,
			To:               nativeTokenAddr,
			Gas:              big.NewInt(0),
			GasPrice:         big.NewInt(0),
			GasUsed:          big.NewInt(0),
			Status:           true,
			GasFeeCap:        big.NewInt(0),
			GasTipCap:        big.NewInt(0),
			Input:            []byte{},
			Nonce:            big.NewInt(0),
			TransactionIndex: txIdx,
			Value:            (*hexutil.Big)(big.NewInt(0)),
		}
		blockFile.Txs = append(blockFile.Txs, nativeTokenTx)

		nativeTokenTraceID := util.ToHash([]string{nativeTokenTxID, "", "0"})
		nativeTokenTrace := ptypes.Trace{
			ID:                nativeTokenTraceID,
			From:              zeroAddr,
			Gas:               big.NewInt(0),
			Input:             []byte{},
			To:                nativeTokenAddr,
			Value:             (*hexutil.Big)(big.NewInt(0)),
			GasUsed:           big.NewInt(0),
			Output:            []byte{},
			CallCreateType:    "create",
			CallType:          "",
			TxID:              nativeTokenTxID,
			ParentTraceID:     "",
			PosInParentTrace:  0,
			SelfStorageChange: false,
			StorageChange:     false,
			Subtraces:         0,
			TraceAddress:      []int64{},
		}
		blockFile.Traces = append(blockFile.Traces, nativeTokenTrace)

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

	config := api.eth.APIBackend.ChainConfig()

	// Mutate the block and state according to any hard-fork specs
	misc.EnsureCreate2Deployer(config, block.Time(), statedb)

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

	rpcTracer.OnBlockStart(block)

	// Process the block using the standard processor
	_, err = api.eth.BlockChain().Processor().Process(block, statedb, vm.Config{Tracer: tracer.Hooks})
	if err != nil {
		return nil, fmt.Errorf("could not process block: %w", err)
	}

	root, destructs, accounts, storages, codes, err := statedb.StateDiff(config.IsEIP158(block.Number()))
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
