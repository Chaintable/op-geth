package eth

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"sync"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
)

const BedrockBlockNumber = 105235063

type DebankOutPut struct {
	BlockFile      *ptypes.BlockFile        `json:"block_file"`
	Header         *ptypes.Header           `json:"header"`
	StateDiff      *ptypes.BlockStorageDiff `json:"state_diff"`
	ValidationHash int64                    `json:"validation_hash"`
}

type DebankAPI struct {
	eth              *Ethereum
	bedrockBlockData *DebankOutPut
	dataReady        bool
	preparing        bool
	preparationError error
	mutex            sync.RWMutex
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	api := &DebankAPI{
		eth:       eth,
		dataReady: false,
		preparing: false,
	}

	go api.prepareBedrockData()

	return api
}

func (api *DebankAPI) getDataDir() string {
	type pathDB interface {
		Path() string
	}
	if pdb, ok := api.eth.chainDb.(pathDB); ok {
		dbPath := pdb.Path()
		dataDir := filepath.Dir(dbPath)
		log.Info("Found dataDir from chainDb", "dbPath", dbPath, "dataDir", dataDir)
		return dataDir
	}

	// 备用方案：使用默认的数据目录
	defaultDir := "/var/data"
	log.Warn("Could not get dataDir from chainDb, using default", "defaultDir", defaultDir)
	return defaultDir
}

type DebankOutPutJs struct {
	BlockFile      *ptypes.BlockFile `json:"block_file"`
	Header         *ptypes.Header    `json:"header"`
	StateDiff      hexutil.Bytes     `json:"state_diff"`
	ValidationHash int64             `json:"validation_hash"`
}

func (api *DebankAPI) DebankBlockRaw(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPut, error) {
	var blockNumber uint64
	if number, ok := blockNrOrHash.Number(); ok {
		blockNumber = uint64(number.Int64())
	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block := api.eth.blockchain.GetBlockByHash(hash)
		if block == nil {
			return nil, fmt.Errorf("could not find block %v", blockNrOrHash)
		}
		blockNumber = block.NumberU64()
	} else {
		return nil, fmt.Errorf("invalid block number or hash")
	}

	if blockNumber != BedrockBlockNumber {
		return nil, fmt.Errorf("unsupported block number, only block %d is supported", BedrockBlockNumber)
	}

	api.mutex.RLock()
	defer api.mutex.RUnlock()

	if !api.dataReady {
		return nil, fmt.Errorf("bedrock block data is still preparing, please wait")
	}

	if api.preparationError != nil {
		return nil, fmt.Errorf("bedrock block data preparation failed: %w", api.preparationError)
	}

	return api.bedrockBlockData, nil
}

func (api *DebankAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPutJs, error) {
	output, err := api.DebankBlockRaw(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}

	data, err := rlp.EncodeToBytes(output.StateDiff)
	if err != nil {
		return nil, err
	}

	return &DebankOutPutJs{
		BlockFile:      output.BlockFile,
		Header:         output.Header,
		StateDiff:      hexutil.Bytes(data),
		ValidationHash: output.ValidationHash,
	}, nil
}

func (api *DebankAPI) prepareBedrockData() {
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
			api.bedrockBlockData = output
		}
		api.mutex.Unlock()

		if err != nil {
			log.Error("Bedrock block data preparation failed", "blockNumber", BedrockBlockNumber, "error", err)
		} else {
			log.Info("Bedrock block data preparation completed", "blockNumber", BedrockBlockNumber)
		}
	}()

	log.Info("Starting to prepare bedrock block data", "blockNumber", BedrockBlockNumber)

	block := api.eth.blockchain.GetBlockByNumber(BedrockBlockNumber)
	if block == nil {
		err = fmt.Errorf("failed to get bedrock block %d", BedrockBlockNumber)
		return
	}

	stateDB, dbErr := api.eth.blockchain.StateAt(block.Root())
	if dbErr != nil {
		err = fmt.Errorf("failed to get state at bedrock block: %w", dbErr)
		return
	}

	dump := stateDB.RawDump2(&state.DumpConfig{
		SkipCode:          false,
		SkipStorage:       false,
		OnlyWithAddresses: false,
	}, api.getDataDir())

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

func convertStateDumpToBlockStorageDiff(dump state.Dump, block *coretypes.Block) *ptypes.BlockStorageDiff {
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
			// 没有 preimage 的账户，直接使用存储的地址哈希
			hashStr := strings.TrimPrefix(addrStr, "pre(")
			hashStr = strings.TrimSuffix(hashStr, ")")
			addrHash = common.HexToHash(hashStr)
			withoutPreimageCount++
		} else {
			// 有 preimage 的账户，从地址计算哈希
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

func buildPipelineBlockFile(block *coretypes.Block) *ptypes.BlockFile {
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

func buildPipelineHeader(block *coretypes.Block) *ptypes.Header {
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
