package util

import (
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
)

func BuildPipelineBlock(rawBlock *types.Block) ptypes.Block {
	block := ptypes.Block{
		ID:                    rawBlock.Hash().Hex(),
		Height:                rawBlock.Number(),
		ParentID:              rawBlock.ParentHash().Hex(),
		BaseFeePerGas:         big.NewInt(0),
		Miner:                 strings.ToLower(rawBlock.Coinbase().Hex()),
		GasLimit:              big.NewInt(int64(rawBlock.GasLimit())),
		GasUsed:               big.NewInt(int64(rawBlock.GasUsed())),
		Timestamp:             rawBlock.Time(),
		ProcessStartTimestamp: time.Now().UnixMilli(),
	}
	if rawBlock.Header().BaseFee != nil {
		block.BaseFeePerGas = rawBlock.Header().BaseFee
	}
	return block
}

func BuildPilelineBlockHeader(block *types.Block) *ptypes.Header {
	blockHeader := ptypes.Header{
		Number:           (*hexutil.Big)(block.Number()),
		Hash:             block.Hash(),
		ParentHash:       block.ParentHash(),
		Nonce:            block.Header().Nonce,
		MixHash:          block.MixDigest(),
		Sha3Uncles:       block.UncleHash(),
		LogsBloom:        block.Bloom(),
		StateRoot:        block.Root(),
		Miner:            block.Coinbase(),
		Difficulty:       (*hexutil.Big)(block.Difficulty()),
		ExtraData:        hexutil.Bytes(block.Extra()),
		GasLimit:         hexutil.Uint64(block.GasLimit()),
		GasUsed:          hexutil.Uint64(block.GasUsed()),
		Timestamp:        hexutil.Uint64(block.Time()),
		TransactionsRoot: block.TxHash(),
		ReceiptsRoot:     block.ReceiptHash(),
	}
	if block.Header().BaseFee != nil {
		blockHeader.BaseFeePerGas = (*hexutil.Big)(block.Header().BaseFee)
	}
	// Note: WithdrawalsHash, BlobGasUsed, ExcessBlobGas, and ParentBeaconRoot are not available in go-ethereum v1.10.19
	//if block.Header().WithdrawalsHash != nil {
	//	blockHeader.WithdrawalsRoot = block.Header().WithdrawalsHash
	//}
	//if block.Header().BlobGasUsed != nil {
	//	blockHeader.BlobGasUsed = (*hexutil.Uint64)(block.Header().BlobGasUsed)
	//}
	//if block.Header().ExcessBlobGas != nil {
	//	blockHeader.ExcessBlobGas = (*hexutil.Uint64)(block.Header().ExcessBlobGas)
	//}
	//if block.Header().ParentBeaconRoot != nil {
	//	blockHeader.ParentBeaconBlockRoot = block.Header().ParentBeaconRoot
	//}
	return &blockHeader
}

func BuildPipelineTransaction(tx *types.Transaction, receipt *types.Receipt, from common.Address, baseFee *big.Int) ptypes.Transaction {
	to := receipt.ContractAddress
	if tx.To() != nil {
		to = *tx.To()
	}
	// Note: EffectiveGasPrice is not available in go-ethereum v1.10.19
	gasPrice := tx.GasPrice()
	transaction := ptypes.Transaction{
		ID:               tx.Hash().Hex(),
		From:             strings.ToLower(from.Hex()),
		To:               strings.ToLower(to.Hex()),
		Gas:              big.NewInt(int64(tx.Gas())),
		GasPrice:         gasPrice,
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        common.Big0,
		GasTipCap:        common.Big0,
		Input:            tx.Data(),
		Nonce:            big.NewInt(int64(tx.Nonce())),
		TransactionIndex: int64(receipt.TransactionIndex),
		Value:            (*hexutil.Big)(tx.Value()),
	}
	switch tx.Type() {
	case types.DynamicFeeTxType:
		// Note: BlobTxType is not available in go-ethereum v1.10.19
		transaction.GasFeeCap = tx.GasFeeCap()
		transaction.GasTipCap = tx.GasTipCap()
	}
	return transaction
}
