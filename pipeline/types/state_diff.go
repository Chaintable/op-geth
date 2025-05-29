package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

type NewAccount struct {
	Address  common.Hash
	Balance  *uint256.Int
	Nonce    uint64
	CodeHash common.Hash
}

type NewCode struct {
	CodeHash common.Hash
	Code     []byte
}

type IndexValuePair struct {
	Index common.Hash
	Value *uint256.Int
}

type AccountStorageDiff struct {
	Address common.Hash
	Values  []IndexValuePair
}

type BlockStorageDiff struct {
	Hash            common.Hash
	ParentHash      common.Hash
	NewAccounts     []NewAccount
	DeletedAccounts []common.Hash
	StorageDiff     []AccountStorageDiff
	NewCodes        []NewCode
}

func (a *BlockStorageDiff) Equal(b *BlockStorageDiff) bool {
	if a.Hash != b.Hash || a.ParentHash != b.ParentHash {
		return false
	}
	newAccounts := make(map[common.Hash]NewAccount, len(a.NewAccounts))
	for _, acc := range a.NewAccounts {
		newAccounts[acc.Address] = acc
	}
	for _, acc := range b.NewAccounts {
		if _, exists := newAccounts[acc.Address]; !exists {
			return false
		}
		if newAccounts[acc.Address].Balance.Cmp(acc.Balance) != 0 ||
			newAccounts[acc.Address].Nonce != acc.Nonce || newAccounts[acc.Address].CodeHash != acc.CodeHash {

			return false
		}
	}
	deletedAccounts := make(map[common.Hash]struct{}, len(a.DeletedAccounts))
	for _, acc := range a.DeletedAccounts {
		deletedAccounts[acc] = struct{}{}
	}
	for _, acc := range b.DeletedAccounts {
		if _, exists := deletedAccounts[acc]; !exists {
			return false
		}
	}

	storageDiff := make(map[common.Hash][]IndexValuePair, len(a.StorageDiff))
	for _, diff := range a.StorageDiff {
		storageDiff[diff.Address] = diff.Values
	}
	for _, diff := range b.StorageDiff {
		values, exists := storageDiff[diff.Address]
		if !exists {
			return false
		}
		storges := make(map[common.Hash]*uint256.Int, len(values))
		for _, v := range values {
			storges[v.Index] = v.Value
		}
		for _, v := range diff.Values {
			if value, exists := storges[v.Index]; !exists || value.Cmp(v.Value) != 0 {
				return false
			}
		}
	}
	newCodes := make(map[common.Hash][]byte, len(a.NewCodes))
	for _, code := range a.NewCodes {
		newCodes[code.CodeHash] = code.Code
	}
	for _, code := range b.NewCodes {
		if c, exists := newCodes[code.CodeHash]; !exists || len(c) != len(code.Code) || string(c) != string(code.Code) {
			return false
		}
	}

	return true
}

type Header struct {
	Number                *hexutil.Big     `json:"number"`
	Hash                  common.Hash      `json:"hash"`
	ParentHash            common.Hash      `json:"parentHash"`
	Nonce                 types.BlockNonce `json:"nonce"`
	MixHash               common.Hash      `json:"mixHash"`
	Sha3Uncles            common.Hash      `json:"sha3Uncles"`
	LogsBloom             types.Bloom      `json:"logsBloom"`
	StateRoot             common.Hash      `json:"stateRoot"`
	Miner                 common.Address   `json:"miner"`
	Difficulty            *hexutil.Big     `json:"difficulty"`
	ExtraData             hexutil.Bytes    `json:"extraData"`
	GasLimit              hexutil.Uint64   `json:"gasLimit"`
	GasUsed               hexutil.Uint64   `json:"gasUsed"`
	Timestamp             hexutil.Uint64   `json:"timestamp"`
	TransactionsRoot      common.Hash      `json:"transactionsRoot"`
	ReceiptsRoot          common.Hash      `json:"receiptsRoot"`
	BaseFeePerGas         *hexutil.Big     `json:"baseFeePerGas,omitempty"`
	WithdrawalsRoot       *common.Hash     `json:"withdrawalsRoot,omitempty"`
	BlobGasUsed           *hexutil.Uint64  `json:"blobGasUsed,omitempty"`
	ExcessBlobGas         *hexutil.Uint64  `json:"excessBlobGas,omitempty"`
	ParentBeaconBlockRoot *common.Hash     `json:"parentBeaconBlockRoot,omitempty"`
	RequestsRoot          *common.Hash     `json:"requestsRoot,omitempty"`
}
