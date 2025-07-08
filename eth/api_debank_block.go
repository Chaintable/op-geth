package eth

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/debank/tracer"
	dtypes "github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
)

// DebankOutPut represents the complete output structure for a block trace
type DebankOutPut struct {
	BlockFile      *dtypes.BlockFile        `json:"block_file"`
	Header         *dtypes.Header           `json:"header"`
	StateDiff      *dtypes.BlockStorageDiff `json:"state_diff"`
	ValidationHash int64                    `json:"validation_hash"`
}

// DebankOutPutJs represents the JSON-serialized output with RLP-encoded state diff
type DebankOutPutJs struct {
	BlockFile      *dtypes.BlockFile `json:"block_file"`
	Header         *dtypes.Header    `json:"header"`
	StateDiff      hexutil.Bytes     `json:"state_diff"`
	ValidationHash int64             `json:"validation_hash"`
}

// DebankAPI provides the debank_* RPC API
type DebankAPI struct {
	eth *Ethereum
}

// NewDebankAPI creates a new DebankAPI instance
func NewDebankAPI(eth *Ethereum) *DebankAPI {
	return &DebankAPI{
		eth: eth,
	}
}

// DebankBlockRaw returns the raw debank block data for the specified block
func (api *DebankAPI) DebankBlockRaw(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPut, error) {
	// Get block number from the input parameter
	var blockNumber uint64
	if number, ok := blockNrOrHash.Number(); ok {
		if number == rpc.LatestBlockNumber {
			blockNumber = api.eth.blockchain.CurrentBlock().Number.Uint64()
		} else if number == rpc.PendingBlockNumber {
			return nil, fmt.Errorf("pending block not supported")
		} else if number == rpc.SafeBlockNumber {
			blockNumber = api.eth.blockchain.CurrentSafeBlock().Number.Uint64()
		} else if number == rpc.FinalizedBlockNumber {
			blockNumber = api.eth.blockchain.CurrentFinalBlock().Number.Uint64()
		} else {
			blockNumber = uint64(number.Int64())
		}
	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block := api.eth.blockchain.GetBlockByHash(hash)
		if block == nil {
			return nil, fmt.Errorf("could not find block %v", blockNrOrHash)
		}
		blockNumber = block.NumberU64()
	} else {
		return nil, fmt.Errorf("invalid block number or hash")
	}

	// Get the block
	block := api.eth.blockchain.GetBlockByNumber(blockNumber)
	if block == nil {
		return nil, fmt.Errorf("could not find block %d", blockNumber)
	}

	// Handle genesis block specially
	if blockNumber == 0 {
		return api.processGenesisBlock(block)
	}

	// Process regular block
	return api.processRegularBlock(block)
}

// DebankBlock returns the JSON-serialized debank block data
func (api *DebankAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPutJs, error) {
	output, err := api.DebankBlockRaw(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}

	// Encode state diff to RLP
	data, err := rlp.EncodeToBytes(output.StateDiff)
	if err != nil {
		return nil, fmt.Errorf("failed to encode state diff: %w", err)
	}

	return &DebankOutPutJs{
		BlockFile:      output.BlockFile,
		Header:         output.Header,
		StateDiff:      hexutil.Bytes(data),
		ValidationHash: output.ValidationHash,
	}, nil
}

// processGenesisBlock handles the special case of genesis block
func (api *DebankAPI) processGenesisBlock(block *coretypes.Block) (*DebankOutPut, error) {
	log.Info("Processing genesis block", "number", block.NumberU64(), "hash", block.Hash().Hex())

	// Get the state at genesis block
	stateDB, err := api.eth.blockchain.StateAt(block.Root())
	if err != nil {
		return nil, fmt.Errorf("failed to get state at genesis block: %w", err)
	}

	// Dump the entire state for genesis block
	dump := stateDB.RawDump(&state.DumpConfig{
		SkipCode:          false,
		SkipStorage:       false,
		OnlyWithAddresses: false,
	})

	log.Info("Genesis state dump completed", "accounts", len(dump.Accounts))

	// Convert state dump to storage diff
	stateDiff := convertStateDumpToBlockStorageDiff(dump, block)

	// Build block file (empty for genesis, no transactions)
	blockFile := buildPipelineBlockFile(block)

	// Build header
	header := buildPipelineHeader(block)

	output := &DebankOutPut{
		BlockFile:      blockFile,
		Header:         header,
		StateDiff:      stateDiff,
		ValidationHash: dtypes.Validation(blockFile).ValidationHash,
	}

	log.Info("Genesis block processing completed",
		"number", block.NumberU64(),
		"accounts", len(stateDiff.NewAccounts),
		"codes", len(stateDiff.NewCodes),
		"validationHash", output.ValidationHash)

	return output, nil
}

// processRegularBlock handles normal block execution and tracing
func (api *DebankAPI) processRegularBlock(block *coretypes.Block) (*DebankOutPut, error) {
	// Get chain config
	chainConfig := api.eth.blockchain.Config()

	// Get parent block
	parent := api.eth.blockchain.GetBlockByHash(block.ParentHash())
	if parent == nil {
		return nil, fmt.Errorf("could not find parent block %s", block.ParentHash().Hex())
	}

	// Get state at parent block
	statedb, err := api.eth.blockchain.StateAt(parent.Root())
	if err != nil {
		return nil, fmt.Errorf("could not get state at parent block: %w", err)
	}

	// Prepare state database for this block - use minimal preparation
	rules := chainConfig.Rules(block.Number(), block.Time() > 0, block.Time())
	statedb.Prepare(rules, common.Address{}, common.Address{}, nil, []common.Address{}, coretypes.AccessList{})

	// Create state tracer
	stateTracer := tracer.NewStateTracer()

	// Initialize block file
	blockFile := buildPipelineBlockFile(block)
	blockFile.Block.ProcessStartTimestamp = time.Now().Unix()

	// Process transactions
	txs := block.Transactions()
	for i, tx := range txs {
		receipt, err := api.processTransaction(statedb, block, tx, i, blockFile, stateTracer, chainConfig)
		if err != nil {
			log.Error("Failed to process transaction", "txHash", tx.Hash().Hex(), "error", err)
			continue
		}

		// Add transaction to block file
		blockFile.Txs = append(blockFile.Txs, convertTransaction(tx, receipt, int64(i)))
	}

	// Finalize block execution
	err = api.finalizeBlockExecution(statedb, block, chainConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to finalize block execution: %w", err)
	}

	// Generate state diff
	stateDiff := stateTracer.GenerateStateDiff(block.Root(), block.ParentHash())

	// Build header
	header := buildPipelineHeader(block)

	// Create final output
	output := &DebankOutPut{
		BlockFile:      blockFile,
		Header:         header,
		StateDiff:      stateDiff,
		ValidationHash: dtypes.Validation(blockFile).ValidationHash,
	}

	return output, nil
}

// processTransaction processes a single transaction with tracing
func (api *DebankAPI) processTransaction(statedb *state.StateDB, block *coretypes.Block, tx *coretypes.Transaction, txIndex int, blockFile *dtypes.BlockFile, stateTracer *tracer.StateTracer, chainConfig *params.ChainConfig) (*coretypes.Receipt, error) {
	// Create call tracer
	callTracer := tracer.NewCallTracer(blockFile, tx.Hash().Hex(), stateTracer)

	// Set up VM config with tracer
	vmConfig := vm.Config{
		Tracer: callTracer,
	}

	// Pre-state capture simplified for opbnb-geth compatibility

	// Create message from transaction
	msg, err := core.TransactionToMessage(tx, coretypes.LatestSignerForChainID(tx.ChainId()), block.BaseFee())
	if err != nil {
		return nil, fmt.Errorf("failed to convert transaction to message: %w", err)
	}

	// Create block context
	blockContext := core.NewEVMBlockContext(block.Header(), api.eth.blockchain, nil, chainConfig, statedb)

	// Create transaction context
	txContext := core.NewEVMTxContext(msg)

	// Create EVM
	vmenv := vm.NewEVM(blockContext, txContext, statedb, chainConfig, vmConfig)

	// Apply transaction
	result, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(block.GasLimit()))
	if err != nil {
		log.Warn("Transaction execution failed", "txHash", tx.Hash().Hex(), "error", err)
	}

	// Create receipt
	receipt := &coretypes.Receipt{
		Type:              tx.Type(),
		PostState:         statedb.IntermediateRoot(chainConfig.IsEIP158(block.Number())).Bytes(),
		Status:            1, // Will be set based on result
		CumulativeGasUsed: result.UsedGas,
		Bloom:             coretypes.CreateBloom(coretypes.Receipts{&coretypes.Receipt{Logs: statedb.GetLogs(tx.Hash(), block.NumberU64(), block.Hash())}}),
		Logs:              statedb.GetLogs(tx.Hash(), block.NumberU64(), block.Hash()),
		TxHash:            tx.Hash(),
		ContractAddress:   common.Address{},
		GasUsed:           result.UsedGas,
		BlockHash:         block.Hash(),
		BlockNumber:       block.Number(),
		TransactionIndex:  uint(txIndex),
	}

	// Set status based on execution result
	if result.Failed() {
		receipt.Status = 0
	}

	// Set contract address for contract creation
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(msg.From, tx.Nonce())
	}

	// Capture post-state
	stateTracer.CapturePostState(statedb, receipt)

	// Process logs as events
	for _, eventLog := range receipt.Logs {
		if callTracer != nil {
			// Use the first trace ID as parent (transaction level)
			parentTraceID := tx.Hash().Hex()
			callTracer.ProcessLog(eventLog, parentTraceID)
		}
	}

	return receipt, nil
}

// finalizeBlockExecution finalizes the block execution (coinbase reward, etc.)
func (api *DebankAPI) finalizeBlockExecution(statedb *state.StateDB, block *coretypes.Block, chainConfig *params.ChainConfig) error {
	// Award coinbase reward if needed
	// Note: In OP Stack, block rewards are typically handled differently
	// but we'll include this for completeness

	// Finalize state
	statedb.Finalise(true)

	return nil
}

// convertTransaction converts a transaction and receipt to pipeline format
func convertTransaction(tx *coretypes.Transaction, receipt *coretypes.Receipt, txIndex int64) dtypes.Transaction {
	var to string
	if tx.To() != nil {
		to = tx.To().Hex()
	} else if receipt.ContractAddress != (common.Address{}) {
		to = receipt.ContractAddress.Hex()
	}

	// Get sender
	sender, _ := coretypes.Sender(coretypes.LatestSignerForChainID(tx.ChainId()), tx)

	return dtypes.Transaction{
		ID:               tx.Hash().Hex(),
		From:             sender.Hex(),
		To:               to,
		Gas:              big.NewInt(int64(tx.Gas())),
		GasPrice:         tx.GasPrice(),
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == 1,
		GasFeeCap:        tx.GasFeeCap(),
		GasTipCap:        tx.GasTipCap(),
		Input:            hexutil.Bytes(tx.Data()),
		Nonce:            big.NewInt(int64(tx.Nonce())),
		TransactionIndex: txIndex,
		Value:            (*hexutil.Big)(tx.Value()),
	}
}

// convertStateDumpToBlockStorageDiff converts a state dump to pipeline BlockStorageDiff format
func convertStateDumpToBlockStorageDiff(dump state.Dump, block *coretypes.Block) *dtypes.BlockStorageDiff {
	diff := &dtypes.BlockStorageDiff{
		Hash:            block.Root(),
		ParentHash:      block.ParentHash(),
		NewAccounts:     make([]dtypes.NewAccount, 0, len(dump.Accounts)),
		DeletedAccounts: make([]common.Hash, 0),
		StorageDiff:     make([]dtypes.AccountStorageDiff, 0),
		NewCodes:        make([]dtypes.NewCode, 0),
	}

	codeHashSet := make(map[common.Hash]struct{})
	withPreimageCount := 0
	withoutPreimageCount := 0

	for addrStr, acc := range dump.Accounts {
		var addrHash common.Hash

		// Handle addresses with and without preimage
		if len(addrStr) > 2 && addrStr[:2] == "0x" && len(addrStr) == 42 {
			// Address with preimage - calculate hash
			addr := common.HexToAddress(addrStr)
			addrHash = crypto.Keccak256Hash(addr.Bytes())
			withPreimageCount++
		} else {
			// Address without preimage - use stored hash
			addrHash = common.HexToHash(addrStr)
			withoutPreimageCount++
		}

		// Parse balance
		balance := uint256.NewInt(0)
		if acc.Balance != "" && acc.Balance != "0" {
			balance = uint256.MustFromDecimal(acc.Balance)
		}

		// Parse code hash
		var codeHash common.Hash
		if len(acc.CodeHash) > 0 {
			codeHash = common.BytesToHash(acc.CodeHash)
		} else {
			codeHash = crypto.Keccak256Hash(nil) // Empty code hash
		}

		// Add new account
		diff.NewAccounts = append(diff.NewAccounts, dtypes.NewAccount{
			Address:  addrHash,
			Balance:  balance,
			Nonce:    acc.Nonce,
			CodeHash: codeHash,
		})

		// Add code if exists and not already added
		if len(acc.Code) > 0 {
			if _, exists := codeHashSet[codeHash]; !exists {
				codeHashSet[codeHash] = struct{}{}
				diff.NewCodes = append(diff.NewCodes, dtypes.NewCode{
					CodeHash: codeHash,
					Code:     acc.Code,
				})
			}
		}

		// Add storage if exists
		if len(acc.Storage) > 0 {
			values := make([]dtypes.IndexValuePair, 0, len(acc.Storage))
			for storageKeyHash, valueStr := range acc.Storage {
				value := uint256.NewInt(0)
				if valueStr != "" && valueStr != "0x" {
					valueBytes := common.Hex2Bytes(valueStr)
					if len(valueBytes) > 0 {
						value = uint256.NewInt(0).SetBytes(valueBytes)
					}
				}

				values = append(values, dtypes.IndexValuePair{
					Index: storageKeyHash,
					Value: value,
				})
			}

			if len(values) > 0 {
				diff.StorageDiff = append(diff.StorageDiff, dtypes.AccountStorageDiff{
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

// buildPipelineBlockFile creates a pipeline BlockFile from a block
func buildPipelineBlockFile(block *coretypes.Block) *dtypes.BlockFile {
	return &dtypes.BlockFile{
		Block: dtypes.Block{
			ID:                    block.Hash().Hex(),
			Height:                big.NewInt(int64(block.NumberU64())),
			ParentID:              block.ParentHash().Hex(),
			GasLimit:              big.NewInt(int64(block.GasLimit())),
			GasUsed:               big.NewInt(int64(block.GasUsed())),
			Miner:                 block.Coinbase().Hex(),
			Timestamp:             block.Time(),
			ProcessStartTimestamp: 0, // Will be set during processing
			BaseFeePerGas:         block.BaseFee(),
		},
		Txs:              make([]dtypes.Transaction, 0),
		Events:           make([]dtypes.Event, 0),
		Traces:           make([]dtypes.Trace, 0),
		ErrorEvents:      make([]dtypes.Event, 0),
		ErrorTraces:      make([]dtypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
}

// buildPipelineHeader creates a pipeline Header from a block header
func buildPipelineHeader(block *coretypes.Block) *dtypes.Header {
	header := block.Header()
	return &dtypes.Header{
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
