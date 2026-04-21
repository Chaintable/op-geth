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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type DebankAPI struct {
	eth *Ethereum
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	return &DebankAPI{eth: eth}
}

func (api *DebankAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ptypes.DebankOutPut, error) {
	block, err := api.eth.APIBackend.BlockByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found")
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

		zeroAddr := "0x0000000000000000000000000000000000000000"
		txIdx := int64(0)

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

			if len(account.Storage) > 0 {
				blockFile.StorageContracts = append(blockFile.StorageContracts, addrLower)
			}

			if account.Balance != nil && account.Balance.Sign() > 0 {
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

			if len(account.Code) > 0 {
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

				traceID := util.ToHash([]string{txID, "", "0"})
				trace := ptypes.Trace{
					ID:                traceID,
					From:              zeroAddr,
					Gas:               big.NewInt(0),
					Input:             account.Code,
					To:                addrLower,
					Value:             (*hexutil.Big)(big.NewInt(0)),
					GasUsed:           big.NewInt(0),
					Output:            account.Code,
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

	parent := api.eth.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent block not found")
	}
	statedb, err := api.eth.blockchain.StateAt(parent.Root())
	if err != nil {
		return nil, err
	}

	rpcTracer := ptracer.RPCTracer{}
	vmConfig := vm.Config{
		Tracer: &rpcTracer,
	}

	rpcTracer.OnBlockStart(block)

	chainConfig := api.eth.blockchain.Config()

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	if chainConfig.PreContractForkBlock != nil && chainConfig.PreContractForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyPreContractHardFork(statedb)
	}

	var (
		txs     = block.Transactions()
		header  = block.Header()
		signer  = types.MakeSigner(chainConfig, header.Number, header.Time)
		gp      = new(core.GasPool).AddGas(block.GasLimit())
		usedGas = new(uint64)
	)

	misc.EnsureCreate2Deployer(chainConfig, header.Time, statedb)

	blockContext := core.NewEVMBlockContext(header, api.eth.blockchain, nil, chainConfig, statedb)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, chainConfig, vmConfig)

	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		core.ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}

	for i, tx := range txs {
		statedb.SetTxContext(tx.Hash(), i)

		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		rpcTracer.OnTxStart(tx, msg.From)

		txContext := core.NewEVMTxContext(msg)
		vmenv.Reset(txContext, statedb)

		result, err := core.ApplyMessage(vmenv, msg, gp)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		var root []byte
		if chainConfig.IsByzantium(header.Number) {
			statedb.Finalise(true)
		} else {
			root = statedb.IntermediateRoot(chainConfig.IsEIP158(header.Number)).Bytes()
		}
		*usedGas += result.UsedGas

		receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas

		if msg.To == nil {
			receipt.ContractAddress = crypto.CreateAddress(vmenv.TxContext.Origin, tx.Nonce())
		}

		receipt.Logs = statedb.GetLogs(tx.Hash(), header.Number.Uint64(), block.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockHash = block.Hash()
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(statedb.TxIndex())

		rpcTracer.OnTxEnd(receipt, nil)
	}

	api.eth.engine.Finalize(api.eth.blockchain, header, statedb, block.Transactions(), block.Uncles(), block.Withdrawals())

	root, destructs, accounts, storages, codes, err := statedb.StateDiff(chainConfig.IsEIP158(block.Number()))
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
