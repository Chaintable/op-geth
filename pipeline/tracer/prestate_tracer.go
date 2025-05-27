package tracer

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	ptypes "github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/holiman/uint256"
)

type stateMap = map[common.Address]*account

type account struct {
	Balance *big.Int                    `json:"balance,omitempty"`
	Code    []byte                      `json:"code,omitempty"`
	Nonce   uint64                      `json:"nonce,omitempty"`
	Storage map[common.Hash]common.Hash `json:"storage,omitempty"`
	empty   bool
}

func (a *account) exists() bool {
	return a.Nonce > 0 || len(a.Code) > 0 || len(a.Storage) > 0 || (a.Balance != nil && a.Balance.Sign() != 0)
}

type prestateTracer struct {
	env       *tracing.VMContext
	pre       stateMap
	post      stateMap
	to        common.Address
	config    prestateTracerConfig
	interrupt atomic.Bool // Atomic flag to signal execution interruption
	reason    error       // Textual reason for the interruption
	created   map[common.Address]bool
	deleted   map[common.Address]bool
}

type prestateTracerConfig struct {
	DiffMode       bool `json:"diffMode"`       // If true, this tracer will return state modifications
	DisableCode    bool `json:"disableCode"`    // If true, this tracer will not return the contract code
	DisableStorage bool `json:"disableStorage"` // If true, this tracer will not return the contract storage
}

func newPrestateTracer(config *prestateTracerConfig) *prestateTracer {
	t := &prestateTracer{
		pre:     stateMap{},
		post:    stateMap{},
		config:  *config,
		created: make(map[common.Address]bool),
		deleted: make(map[common.Address]bool),
	}
	return t
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *prestateTracer) OnTxStart(env *tracing.VMContext, tx *types.Transaction, from common.Address) {
	t.env = env
	if tx.To() == nil {
		t.to = crypto.CreateAddress(from, env.StateDB.GetNonce(from))
		t.created[t.to] = true
	} else {
		t.to = *tx.To()
	}

	t.lookupAccount(from)
	t.lookupAccount(t.to)
	t.lookupAccount(env.Coinbase)
}

func (t *prestateTracer) OnSystemCallStartHookV2(env *tracing.VMContext) {
	t.env = env
	// Initialize the prestate for the system call origin
	t.lookupAccount(params.BeaconRootsStorageAddress)
}

func (t *prestateTracer) processDiffState() {
	for addr, state := range t.pre {
		// The deleted account's state is pruned from `post` but kept in `pre`
		if _, ok := t.deleted[addr]; ok {
			continue
		}
		modified := false
		postAccount := &account{Storage: make(map[common.Hash]common.Hash)}
		newBalance := t.env.StateDB.GetBalance(addr).ToBig()
		newNonce := t.env.StateDB.GetNonce(addr)

		log.Info("prestateTracer.processDiffState", "address", addr.Hex(), "preBalance", t.pre[addr].Balance, "balance", newBalance, "preNonce", t.pre[addr].Nonce, "nonce", newNonce)

		if newBalance.Cmp(t.pre[addr].Balance) != 0 {
			modified = true
			postAccount.Balance = newBalance
		}
		if newNonce != t.pre[addr].Nonce {
			modified = true
			postAccount.Nonce = newNonce
		}
		if !t.config.DisableCode {
			newCode := t.env.StateDB.GetCode(addr)
			if !bytes.Equal(newCode, t.pre[addr].Code) {
				modified = true
				postAccount.Code = newCode
			}
		}

		if !t.config.DisableStorage {
			for key, val := range state.Storage {
				// don't include the empty slot
				if val == (common.Hash{}) {
					delete(t.pre[addr].Storage, key)
				}

				newVal := t.env.StateDB.GetState(addr, key)
				if val == newVal {
					// Omit unchanged slots
					delete(t.pre[addr].Storage, key)
				} else {
					modified = true
					if newVal != (common.Hash{}) {
						postAccount.Storage[key] = newVal
					}
				}
			}
		}

		if modified {
			t.post[addr] = postAccount
		} else {
			// if state is not modified, then no need to include into the pre state
			delete(t.pre, addr)
		}
	}
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (t *prestateTracer) OnTxEnd(receipt *types.Receipt, err error) {
	if err != nil {
		return
	}
	if t.config.DiffMode {
		t.processDiffState()
	}
	// the new created contracts' prestate were empty, so delete them
	for a := range t.created {
		// the created contract maybe exists in statedb before the creating tx
		if s := t.pre[a]; s != nil && s.empty {
			delete(t.pre, a)
		}
	}
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (t *prestateTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if err != nil {
		return
	}
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}
	stack := scope.Stack
	stackData := stack.Data()
	stackLen := len(stackData)
	caller := scope.Contract.Address()
	switch {
	case stackLen >= 1 && (op == vm.SLOAD || op == vm.SSTORE):
		slot := common.Hash(stackData[stackLen-1].Bytes32())
		t.lookupStorage(caller, slot)
	case stackLen >= 1 && (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT):
		addr := common.Address(stackData[stackLen-1].Bytes20())
		t.lookupAccount(addr)
		if op == vm.SELFDESTRUCT {
			t.deleted[caller] = true
		}
	case stackLen >= 5 && (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE):
		addr := common.Address(stackData[stackLen-2].Bytes20())
		t.lookupAccount(addr)
	case op == vm.CREATE:
		nonce := t.env.StateDB.GetNonce(caller)
		addr := crypto.CreateAddress(caller, nonce)
		t.lookupAccount(addr)
		t.created[addr] = true
	case stackLen >= 4 && op == vm.CREATE2:
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init, err := getMemoryCopyPadded(scope.Memory, int64(offset.Uint64()), int64(size.Uint64()))
		if err != nil {
			log.Warn("failed to copy CREATE2 input", "err", err, "tracer", "prestateTracer", "offset", offset, "size", size)
			return
		}
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		addr := crypto.CreateAddress2(caller, salt.Bytes32(), inithash)
		t.lookupAccount(addr)
		t.created[addr] = true
	}
}

func (t *prestateTracer) GetStateDiff(originRoot common.Hash, root common.Hash) *ptypes.BlockStorageDiff {
	stateDiff := &ptypes.BlockStorageDiff{}
	if originRoot == (common.Hash{}) {
		originRoot = types.EmptyRootHash
	}
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	stateDiff.Hash = root
	stateDiff.ParentHash = originRoot

	for addr, account := range t.post {
		// only storage changes
		if account.Nonce == 0 && len(account.Code) == 0 && account.Balance == nil {
			continue
		}
		if account.Balance != nil || account.Nonce != 0 || len(account.Code) > 0 {
			newBalance := t.pre[addr].Balance
			if account.Balance != nil {
				newBalance = account.Balance
			}
			newNonce := t.pre[addr].Nonce
			if account.Nonce != 0 {
				newNonce = account.Nonce
			}
			newCodeHash := crypto.Keccak256Hash(t.pre[addr].Code)
			if len(account.Code) > 0 {
				newCodeHash = crypto.Keccak256Hash(account.Code)
			}

			stateDiff.NewAccounts = append(stateDiff.NewAccounts, ptypes.NewAccount{
				Address:  addressToHash(addr),
				Balance:  uint256.MustFromBig(newBalance),
				Nonce:    newNonce,
				CodeHash: newCodeHash,
			})
		}
	}

	for addr := range t.deleted {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addressToHash(addr))
	}

	for addr, acct := range t.post {
		Values := make([]ptypes.IndexValuePair, 0, len(acct.Storage))
		for k, v := range acct.Storage {
			value := uint256.NewInt(0).SetBytes(v.Bytes())
			Values = append(Values, ptypes.IndexValuePair{
				Index: crypto.Keccak256Hash(k[:]),
				Value: value,
			})
		}
		if len(Values) > 0 {
			stateDiff.StorageDiff = append(stateDiff.StorageDiff, ptypes.AccountStorageDiff{
				Address: addressToHash(addr),
				Values:  Values,
			})
		}
	}

	for _, code := range t.post {
		if len(code.Code) > 0 {
			stateDiff.NewCodes = append(stateDiff.NewCodes, ptypes.NewCode{
				CodeHash: crypto.Keccak256Hash(code.Code),
				Code:     code.Code,
			})
		}
	}
	return stateDiff
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *prestateTracer) Stop(err error) {
	t.reason = err
	t.interrupt.Store(true)
}

// lookupAccount fetches details of an account and adds it to the prestate
// if it doesn't exist there.
func (t *prestateTracer) lookupAccount(addr common.Address) {
	log.Info("prestateTracer.lookupAccount", "address", addr.Hex(), "config", t.config)
	if _, ok := t.pre[addr]; ok {
		return
	}

	acc := &account{
		Balance: t.env.StateDB.GetBalance(addr).ToBig(),
		Nonce:   t.env.StateDB.GetNonce(addr),
		Code:    t.env.StateDB.GetCode(addr),
	}
	if !acc.exists() {
		acc.empty = true
	}
	// The code must be fetched first for the emptiness check.
	if t.config.DisableCode {
		acc.Code = nil
	}
	if !t.config.DisableStorage {
		acc.Storage = make(map[common.Hash]common.Hash)
	}
	t.pre[addr] = acc
}

// lookupStorage fetches the requested storage slot and adds
// it to the prestate of the given contract. It assumes `lookupAccount`
// has been performed on the contract before.
func (t *prestateTracer) lookupStorage(addr common.Address, key common.Hash) {
	if t.config.DisableStorage {
		return
	}
	if _, ok := t.pre[addr].Storage[key]; ok {
		return
	}
	t.pre[addr].Storage[key] = t.env.StateDB.GetState(addr, key)
}

const (
	memoryPadLimit = 1024 * 1024
)

// getMemoryCopyPadded returns offset + size as a new slice.
// It zero-pads the slice if it extends beyond memory bounds.
func getMemoryCopyPadded(m *vm.Memory, offset, size int64) ([]byte, error) {
	if offset < 0 || size < 0 {
		return nil, errors.New("offset or size must not be negative")
	}
	if int(offset+size) < m.Len() { // slice fully inside memory
		return m.GetCopy(offset, size), nil
	}
	paddingNeeded := int(offset+size) - m.Len()
	if paddingNeeded > memoryPadLimit {
		return nil, fmt.Errorf("reached limit for padding memory slice: %d", paddingNeeded)
	}
	cpy := make([]byte, size)
	if overlap := int64(m.Len()) - offset; overlap > 0 {
		copy(cpy, m.GetPtr(offset, overlap))
	}
	return cpy, nil
}

func addressToHash(a common.Address) common.Hash {
	return crypto.HashData(crypto.NewKeccakState(), a.Bytes())
}
