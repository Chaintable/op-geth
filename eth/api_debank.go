package eth

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"strings"
	"sync"

	ptracer "github.com/Chaintable/pipeline/tracer"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
)

const MantleBedrockBlockNumber = 61171946

type DebankAPI struct {
	eth                    *Ethereum
	mantleBedrockBlockData *DebankOutPut
	dataReady              bool
	preparing              bool
	preparationError       error
	mutex                  sync.RWMutex
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	api := &DebankAPI{
		eth:       eth,
		dataReady: false,
		preparing: false,
	}

	return api
}

type DebankOutPut struct {
	BlockFile      *ptypes.BlockFile        `json:"block_file"`
	Header         *ptypes.Header           `json:"header"`
	StateDiff      *ptypes.BlockStorageDiff `json:"state_diff"`
	ValidationHash int64                    `json:"validation_hash"`
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

	// If this is the Mantle bedrock block, use specialized handling
	if block.NumberU64() == MantleBedrockBlockNumber {
		// Trigger preparation if not ready and not preparing
		if !api.dataReady && !api.preparing {
			go api.prepareMantleBedrockData()
		}

		output, err := api.DebankBlockRaw(ctx, blockNrOrHash)
		if err != nil {
			return nil, err
		}

		data, err := rlp.EncodeToBytes(output.StateDiff)
		if err != nil {
			return nil, err
		}

		return &ptypes.DebankOutPut{
			BlockFile:      output.BlockFile,
			Header:         output.Header,
			StateDiff:      hexutil.Bytes(data),
			ValidationHash: output.ValidationHash,
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
		rules := api.eth.APIBackend.ChainConfig().Rules(block.Number(), false, block.Time())
		msg, err := core.TransactionToMessage(tx, signer, blockCtx.BaseFee, &rules)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		statedb.SetTxContext(tx.Hash(), i)

		receipt, err := core.ApplyTransactionWithEVM(msg, gp, statedb, block.Number(), block.Hash(), block.Time(), tx, usedGas, evm)
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

func (api *DebankAPI) DebankBlockRaw(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPut, error) {
	block, err := api.eth.APIBackend.BlockByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}

	if block.NumberU64() != MantleBedrockBlockNumber {
		return nil, fmt.Errorf("unsupported block number, only block %d is supported", MantleBedrockBlockNumber)
	}

	api.mutex.RLock()
	defer api.mutex.RUnlock()

	if !api.dataReady {
		return nil, fmt.Errorf("Mantle bedrock block data is still preparing, please wait")
	}

	if api.preparationError != nil {
		return nil, fmt.Errorf("Mantle bedrock block data preparation failed: %w", api.preparationError)
	}

	return api.mantleBedrockBlockData, nil
}

func (api *DebankAPI) prepareMantleBedrockData() {
	api.mutex.Lock()
	if api.preparing {
		api.mutex.Unlock()
		return
	}
	api.preparing = true
	api.mutex.Unlock()

	var err error
	var output *DebankOutPut

	defer func() {
		api.mutex.Lock()
		api.preparing = false
		api.dataReady = true
		api.preparationError = err
		if output != nil {
			api.mantleBedrockBlockData = output
		}
		api.mutex.Unlock()

		if err != nil {
			log.Error("Mantle bedrock block data preparation failed", "blockNumber", MantleBedrockBlockNumber, "error", err)
		} else {
			log.Info("Mantle bedrock block data preparation completed", "blockNumber", MantleBedrockBlockNumber)
		}
	}()

	log.Info("Starting to prepare Mantle bedrock block data", "blockNumber", MantleBedrockBlockNumber)

	block := api.eth.blockchain.GetBlockByNumber(MantleBedrockBlockNumber)
	if block == nil {
		err = fmt.Errorf("failed to get Mantle bedrock block %d", MantleBedrockBlockNumber)
		return
	}

	stateDB, dbErr := api.eth.blockchain.StateAt(block.Root())
	if dbErr != nil {
		err = fmt.Errorf("failed to get state at Mantle bedrock block: %w", dbErr)
		return
	}

	dump := stateDB.RawDump2(&state.DumpConfig{
		SkipCode:          false,
		SkipStorage:       false,
		OnlyWithAddresses: false,
	}, api.eth.dataDir)

	log.Info("State dump completed", "accounts", len(dump.Accounts))

	stateDiff := convertStateDumpToBlockStorageDiff(dump, block)
	blockFile := buildPipelineBlockFile(block)
	header := buildPipelineHeader(block)

	output = &DebankOutPut{
		BlockFile:      blockFile,
		Header:         header,
		StateDiff:      stateDiff,
		ValidationHash: blockFile.Validation().ValidationHash,
	}
}

func convertStateDumpToBlockStorageDiff(dump state.Dump, block *types.Block) *ptypes.BlockStorageDiff {
	diff := &ptypes.BlockStorageDiff{
		Hash:            block.Root(),
		ParentHash:      block.ParentHash(),
		NewAccounts:     make([]ptypes.NewAccount, 0, len(dump.Accounts)),
		DeletedAccounts: make([]common.Hash, 0),
		StorageDiff:     make([]ptypes.AccountStorageDiff, 0),
		NewCodes:        make([]ptypes.NewCode, 0),
	}

	codeHashSet := make(map[common.Hash]struct{})
	withPreimageCount := 0
	withoutPreimageCount := 0

	for addrStr, acc := range dump.Accounts {
		var addrHash common.Hash

		if strings.HasPrefix(addrStr, "pre(") {
			hashStr := strings.TrimPrefix(addrStr, "pre(")
			hashStr = strings.TrimSuffix(hashStr, ")")
			addrHash = common.HexToHash(hashStr)
			withoutPreimageCount++
		} else {
			addr := common.HexToAddress(addrStr)
			addrHash = crypto.Keccak256Hash(addr.Bytes())
			withPreimageCount++
		}

		balance := uint256.NewInt(0)
		if acc.Balance != "" && acc.Balance != "0" {
			balance = uint256.MustFromDecimal(acc.Balance)
		}

		var codeHash common.Hash
		if len(acc.CodeHash) > 0 {
			codeHash = common.BytesToHash(acc.CodeHash)
		} else {
			codeHash = crypto.Keccak256Hash(nil)
		}

		diff.NewAccounts = append(diff.NewAccounts, ptypes.NewAccount{
			Address:  addrHash,
			Balance:  balance,
			Nonce:    acc.Nonce,
			CodeHash: codeHash,
		})

		if len(acc.Code) > 0 {
			if _, exists := codeHashSet[codeHash]; !exists {
				codeHashSet[codeHash] = struct{}{}
				diff.NewCodes = append(diff.NewCodes, ptypes.NewCode{
					CodeHash: codeHash,
					Code:     acc.Code,
				})
			}
		}

		if len(acc.Storage) > 0 {
			values := make([]ptypes.IndexValuePair, 0, len(acc.Storage))
			for storageKeyHash, valueStr := range acc.Storage {
				value := uint256.NewInt(0)
				if valueStr != "" && valueStr != "0x" {
					valueBytes := common.Hex2Bytes(valueStr)
					if len(valueBytes) > 0 {
						value = uint256.NewInt(0).SetBytes(valueBytes)
					}
				}

				values = append(values, ptypes.IndexValuePair{
					Index: storageKeyHash,
					Value: value,
				})
			}

			if len(values) > 0 {
				diff.StorageDiff = append(diff.StorageDiff, ptypes.AccountStorageDiff{
					Address: addrHash,
					Values:  values,
				})
			}
		}
	}

	log.Info("Converted state dump to storage diff",
		"totalAccounts", len(dump.Accounts),
		"withPreimage", withPreimageCount,
		"withoutPreimage", withoutPreimageCount,
		"newAccounts", len(diff.NewAccounts),
		"newCodes", len(diff.NewCodes),
		"storageDiff", len(diff.StorageDiff))

	return diff
}

func buildPipelineBlockFile(block *types.Block) *ptypes.BlockFile {
	return &ptypes.BlockFile{
		Block: ptypes.Block{
			ID:                    block.Hash().Hex(),
			Height:                big.NewInt(int64(block.NumberU64())),
			ParentID:              block.ParentHash().Hex(),
			GasLimit:              big.NewInt(int64(block.GasLimit())),
			GasUsed:               big.NewInt(int64(block.GasUsed())),
			Miner:                 block.Coinbase().Hex(),
			Timestamp:             block.Time(),
			ProcessStartTimestamp: 0,
			BaseFeePerGas:         block.BaseFee(),
		},
		Txs:              make([]ptypes.Transaction, 0),
		Events:           make([]ptypes.Event, 0),
		Traces:           make([]ptypes.Trace, 0),
		ErrorEvents:      make([]ptypes.Event, 0),
		ErrorTraces:      make([]ptypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
}

func buildPipelineHeader(block *types.Block) *ptypes.Header {
	header := block.Header()
	return &ptypes.Header{
		Number:           (*hexutil.Big)(header.Number),
		Hash:             header.Hash(),
		ParentHash:       header.ParentHash,
		Nonce:            header.Nonce,
		MixHash:          header.MixDigest,
		Sha3Uncles:       header.UncleHash,
		LogsBloom:        header.Bloom,
		StateRoot:        header.Root,
		Miner:            header.Coinbase,
		Difficulty:       (*hexutil.Big)(header.Difficulty),
		ExtraData:        header.Extra,
		GasLimit:         hexutil.Uint64(header.GasLimit),
		GasUsed:          hexutil.Uint64(header.GasUsed),
		Timestamp:        hexutil.Uint64(header.Time),
		TransactionsRoot: header.TxHash,
		ReceiptsRoot:     header.ReceiptHash,
		BaseFeePerGas:    (*hexutil.Big)(header.BaseFee),
	}
}
