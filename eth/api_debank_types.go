package eth

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type DebankBlockContext struct {
	BlockId   rpc.BlockNumberOrHash `json:"block_id"`
	BlockType string                `json:"type"`
}

func (c *DebankBlockContext) GetBlockNumberOrHash() rpc.BlockNumberOrHash {
	if c.BlockType == "Contains" {
		return rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	}
	return c.BlockId
}

type BlockOverrides struct{}

// --- contractMultiCall ---

type DebankSingleCallResult struct {
	Code      int           `json:"code"`
	Err       string        `json:"err"`
	FromCache bool          `json:"from_cache"`
	Result    hexutil.Bytes `json:"result"`
	GasUsed   int64         `json:"gas_used"`
	TimeCost  float64       `json:"time_cost"`
}

type DebankMultiCallStats struct {
	BlockNum     uint64      `json:"block_num"`
	BlockHash    common.Hash `json:"block_hash"`
	BlockTime    uint64      `json:"block_time"`
	Success      bool        `json:"success"`
	CacheEnabled bool        `json:"cache_enabled"`
}

type DebankMultiCallResp struct {
	Results []*DebankSingleCallResult `json:"results"`
	Stats   *DebankMultiCallStats     `json:"stats"`
}

// --- simulateTransactions ---

type DebankTrace struct {
	ID                string         `json:"id"`
	FromAddr          common.Address `json:"from_addr"`
	GasLimit          uint64         `json:"gas_limit"`
	Input             hexutil.Bytes  `json:"input"`
	ToAddr            common.Address `json:"to_addr"`
	Value             *hexutil.Big   `json:"value"`
	GasUsed           uint64         `json:"gas_used"`
	Output            hexutil.Bytes  `json:"output"`
	CallCreateType    string         `json:"type"`
	CallType          string         `json:"call_type"`
	TxID              common.Hash    `json:"tx_id"`
	ParentTraceID     string         `json:"parent_trace_id"`
	PosInParentTrace  int            `json:"pos_in_parent_trace"`
	SelfStorageChange bool           `json:"self_storage_change"`
	StorageChange     bool           `json:"storage_change"`
}

type DebankEvent struct {
	ID               string         `json:"id"`
	ContractID       common.Address `json:"contract_id"`
	Selector         string         `json:"selector"`
	Topics           []string       `json:"topics"`
	Data             hexutil.Bytes  `json:"data"`
	TxID             common.Hash    `json:"tx_id"`
	ParentTraceID    string         `json:"parent_trace_id"`
	PosInParentTrace int            `json:"pos_in_parent_trace"`
}

type DebankSingleSimulateResult struct {
	Traces  []DebankTrace `json:"traces"`
	Events  []DebankEvent `json:"events"`
	Code    int           `json:"code"`
	Err     string        `json:"err"`
	GasUsed uint64        `json:"gas_used"`
}

type DebankSimulateStats struct {
	BlockNum  uint64      `json:"block_num"`
	BlockHash common.Hash `json:"block_hash"`
	BlockTime uint64      `json:"block_time"`
	Success   bool        `json:"success"`
}

type DebankSimulateResp struct {
	Results []DebankSingleSimulateResult `json:"results"`
	Stats   DebankSimulateStats          `json:"stats"`
}
