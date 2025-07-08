package tracer

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/holiman/uint256"
)

type StateTracer struct {
	newAccounts     map[common.Hash]types.NewAccount
	deletedAccounts map[common.Hash]struct{}
	storageDiffs    map[common.Hash]map[common.Hash]*uint256.Int
	newCodes        map[common.Hash]types.NewCode
	mu              sync.RWMutex
}

func NewStateTracer() *StateTracer {
	return &StateTracer{
		newAccounts:     make(map[common.Hash]types.NewAccount),
		deletedAccounts: make(map[common.Hash]struct{}),
		storageDiffs:    make(map[common.Hash]map[common.Hash]*uint256.Int),
		newCodes:        make(map[common.Hash]types.NewCode),
	}
}

func (st *StateTracer) OnAccountCreated(addr common.Address, balance *uint256.Int, nonce uint64, codeHash common.Hash) {
	st.mu.Lock()
	defer st.mu.Unlock()

	addrHash := crypto.Keccak256Hash(addr.Bytes())

	st.newAccounts[addrHash] = types.NewAccount{
		Address:  addrHash,
		Balance:  balance,
		Nonce:    nonce,
		CodeHash: codeHash,
	}

	log.Debug("Account created", "address", addr.Hex(), "nonce", nonce, "balance", balance)
}

func (st *StateTracer) OnAccountDeleted(addr common.Address) {
	st.mu.Lock()
	defer st.mu.Unlock()

	addrHash := crypto.Keccak256Hash(addr.Bytes())
	st.deletedAccounts[addrHash] = struct{}{}

	log.Debug("Account deleted", "address", addr.Hex())
}

func (st *StateTracer) OnStorageChanged(addr common.Address, key common.Hash, prev, curr common.Hash) {
	st.mu.Lock()
	defer st.mu.Unlock()

	addrHash := crypto.Keccak256Hash(addr.Bytes())

	if st.storageDiffs[addrHash] == nil {
		st.storageDiffs[addrHash] = make(map[common.Hash]*uint256.Int)
	}

	value := uint256.NewInt(0)
	if curr != (common.Hash{}) {
		value.SetBytes(curr.Bytes())
	}

	st.storageDiffs[addrHash][key] = value

	log.Debug("Storage changed", "address", addr.Hex(), "key", key.Hex(), "prev", prev.Hex(), "curr", curr.Hex())
}

func (st *StateTracer) OnCodeChanged(addr common.Address, prevCodeHash, currCodeHash common.Hash, code []byte) {
	st.mu.Lock()
	defer st.mu.Unlock()

	if currCodeHash != (common.Hash{}) && len(code) > 0 {
		st.newCodes[currCodeHash] = types.NewCode{
			CodeHash: currCodeHash,
			Code:     code,
		}

		log.Debug("Code deployed", "address", addr.Hex(), "codeHash", currCodeHash.Hex(), "size", len(code))
	}
}

func (st *StateTracer) CapturePostState(statedb *state.StateDB, receipt *coretypes.Receipt) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// For contract creation, track the new contract
	if receipt.ContractAddress != (common.Address{}) {
		balance := statedb.GetBalance(receipt.ContractAddress)
		nonce := statedb.GetNonce(receipt.ContractAddress)
		codeHash := statedb.GetCodeHash(receipt.ContractAddress)

		addrHash := crypto.Keccak256Hash(receipt.ContractAddress.Bytes())

		balanceUint256 := uint256.NewInt(0)
		if balance != nil {
			balanceUint256.Set((*uint256.Int)(balance))
		}

		st.newAccounts[addrHash] = types.NewAccount{
			Address:  addrHash,
			Balance:  balanceUint256,
			Nonce:    nonce,
			CodeHash: codeHash,
		}

		// Also track the code
		if codeHash != (common.Hash{}) {
			code := statedb.GetCode(receipt.ContractAddress)
			if len(code) > 0 {
				st.newCodes[codeHash] = types.NewCode{
					CodeHash: codeHash,
					Code:     code,
				}
			}
		}
	}
}

func (st *StateTracer) GenerateStateDiff(blockHash, parentHash common.Hash) *types.BlockStorageDiff {
	st.mu.RLock()
	defer st.mu.RUnlock()

	newAccounts := make([]types.NewAccount, 0, len(st.newAccounts))
	for _, account := range st.newAccounts {
		newAccounts = append(newAccounts, account)
	}

	deletedAccounts := make([]common.Hash, 0, len(st.deletedAccounts))
	for addrHash := range st.deletedAccounts {
		deletedAccounts = append(deletedAccounts, addrHash)
	}

	storageDiffs := make([]types.AccountStorageDiff, 0, len(st.storageDiffs))
	for addrHash, storage := range st.storageDiffs {
		if len(storage) == 0 {
			continue
		}

		values := make([]types.IndexValuePair, 0, len(storage))
		for key, value := range storage {
			values = append(values, types.IndexValuePair{
				Index: key,
				Value: value,
			})
		}

		storageDiffs = append(storageDiffs, types.AccountStorageDiff{
			Address: addrHash,
			Values:  values,
		})
	}

	newCodes := make([]types.NewCode, 0, len(st.newCodes))
	for _, code := range st.newCodes {
		newCodes = append(newCodes, code)
	}

	diff := &types.BlockStorageDiff{
		Hash:            blockHash,
		ParentHash:      parentHash,
		NewAccounts:     newAccounts,
		DeletedAccounts: deletedAccounts,
		StorageDiff:     storageDiffs,
		NewCodes:        newCodes,
	}

	log.Info("Generated state diff",
		"blockHash", blockHash.Hex(),
		"newAccounts", len(newAccounts),
		"deletedAccounts", len(deletedAccounts),
		"storageDiffs", len(storageDiffs),
		"newCodes", len(newCodes))

	return diff
}

func (st *StateTracer) Reset() {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.newAccounts = make(map[common.Hash]types.NewAccount)
	st.deletedAccounts = make(map[common.Hash]struct{})
	st.storageDiffs = make(map[common.Hash]map[common.Hash]*uint256.Int)
	st.newCodes = make(map[common.Hash]types.NewCode)
}
