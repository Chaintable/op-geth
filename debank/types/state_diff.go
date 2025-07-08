package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

type NewAccount struct {
	Address  common.Hash  `json:"address"`
	Balance  *uint256.Int `json:"balance"`
	Nonce    uint64       `json:"nonce"`
	CodeHash common.Hash  `json:"code_hash"`
}

type NewCode struct {
	CodeHash common.Hash `json:"code_hash"`
	Code     []byte      `json:"code"`
}

type IndexValuePair struct {
	Index common.Hash  `json:"index"`
	Value *uint256.Int `json:"value"`
}

type AccountStorageDiff struct {
	Address common.Hash      `json:"address"`
	Values  []IndexValuePair `json:"values"`
}

type BlockStorageDiff struct {
	Hash            common.Hash          `json:"hash"`
	ParentHash      common.Hash          `json:"parent_hash"`
	NewAccounts     []NewAccount         `json:"new_accounts"`
	DeletedAccounts []common.Hash        `json:"deleted_accounts"`
	StorageDiff     []AccountStorageDiff `json:"storage_diff"`
	NewCodes        []NewCode            `json:"new_codes"`
}
