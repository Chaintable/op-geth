package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	coretypes "github.com/ethereum/go-ethereum/core/types"
)

type Block struct {
	ID                    string   `json:"id"`
	Height                *big.Int `json:"height"`
	ParentID              string   `json:"parent_id"`
	GasLimit              *big.Int `json:"gas_limit"`
	GasUsed               *big.Int `json:"gas_used"`
	Miner                 string   `json:"miner"`
	Timestamp             uint64   `json:"timestamp"`
	ProcessStartTimestamp int64    `json:"process_start_timestamp"`
	BaseFeePerGas         *big.Int `json:"base_fee_per_gas"`
}

type Transaction struct {
	ID               string        `json:"id"`
	From             string        `json:"from_addr"`
	To               string        `json:"to_addr"`
	Gas              *big.Int      `json:"gas_limit"`
	GasPrice         *big.Int      `json:"gas_price"`
	GasUsed          *big.Int      `json:"gas_used"`
	Status           bool          `json:"status"`
	GasFeeCap        *big.Int      `json:"max_fee_per_gas"`
	GasTipCap        *big.Int      `json:"max_priority_fee_per_gas"`
	Input            hexutil.Bytes `json:"input"`
	Nonce            *big.Int      `json:"nonce"`
	TransactionIndex int64         `json:"idx"`
	Value            *hexutil.Big  `json:"value"`
}

type Event struct {
	ID            string        `json:"id"`
	Address       string        `json:"contract_id"`
	Selector      string        `json:"selector"`
	Topics        []string      `json:"topics"`
	Data          hexutil.Bytes `json:"data"`
	ParentTraceID string        `json:"parent_trace_id"`
	Position      int64         `json:"pos_in_parent_trace"`
	LogIndex      int64         `json:"idx"`
}

type Trace struct {
	ID                string        `json:"id"`
	From              string        `json:"from_addr"`
	Gas               *big.Int      `json:"gas_limit"`
	Input             hexutil.Bytes `json:"input"`
	To                string        `json:"to_addr"`
	Value             *hexutil.Big  `json:"value"`
	GasUsed           *big.Int      `json:"gas_used"`
	Output            hexutil.Bytes `json:"output"`
	CallCreateType    string        `json:"type"`
	CallType          string        `json:"call_type"`
	TxID              string        `json:"tx_id"`
	ParentTraceID     string        `json:"parent_trace_id"`
	PosInParentTrace  int64         `json:"pos_in_parent_trace"`
	SelfStorageChange bool          `json:"self_storage_change"`
	StorageChange     bool          `json:"storage_change"`
	Subtraces         int64         `json:"subtraces"`
	TraceAddress      []int64       `json:"trace_address"`
	Error             string        `json:"error,omitempty"`
}

type BlockFile struct {
	Block            Block         `json:"block"`
	Txs              []Transaction `json:"txs"`
	Events           []Event       `json:"events"`
	Traces           []Trace       `json:"traces"`
	ErrorEvents      []Event       `json:"error_events"`
	ErrorTraces      []Trace       `json:"error_traces"`
	StorageContracts []string      `json:"storage_contracts"`
}

type Header struct {
	Number           *hexutil.Big         `json:"number"`
	Hash             common.Hash          `json:"hash"`
	ParentHash       common.Hash          `json:"parentHash"`
	Nonce            coretypes.BlockNonce `json:"nonce"`
	MixHash          common.Hash          `json:"mixHash"`
	Sha3Uncles       common.Hash          `json:"sha3Uncles"`
	LogsBloom        coretypes.Bloom      `json:"logsBloom"`
	StateRoot        common.Hash          `json:"stateRoot"`
	Miner            common.Address       `json:"miner"`
	Difficulty       *hexutil.Big         `json:"difficulty"`
	ExtraData        hexutil.Bytes        `json:"extraData"`
	GasLimit         hexutil.Uint64       `json:"gasLimit"`
	GasUsed          hexutil.Uint64       `json:"gasUsed"`
	Timestamp        hexutil.Uint64       `json:"timestamp"`
	TransactionsRoot common.Hash          `json:"transactionsRoot"`
	ReceiptsRoot     common.Hash          `json:"receiptsRoot"`
	BaseFeePerGas    *hexutil.Big         `json:"baseFeePerGas,omitempty"`
}
