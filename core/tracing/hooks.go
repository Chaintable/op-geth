package tracing

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// StateDB gives tracers access to the whole state.
type StateDB interface {
	GetBalance(common.Address) *uint256.Int
	GetNonce(common.Address) uint64
	GetCode(common.Address) []byte
	GetCodeHash(common.Address) common.Hash
	GetState(common.Address, common.Hash) common.Hash
	GetTransientState(common.Address, common.Hash) common.Hash
	Exist(common.Address) bool
	GetRefund() uint64
}

// VMContext provides the context for the EVM execution.
type VMContext struct {
	Coinbase    common.Address
	BlockNumber *big.Int
	Time        uint64
	Random      *common.Hash
	BaseFee     *big.Int
	StateDB     StateDB
}

type (
	/*
		- VM events -
	*/

	// TxStartHook is called before the execution of a transaction starts.
	// Call simulations don't come with a valid signature. `from` field
	// to be used for address of the caller.
	TxStartHook = func(vmContext *VMContext, tx *types.Transaction, from common.Address)

	// TxEndHook is called after the execution of a transaction ends.
	TxEndHook = func(receipt *types.Receipt, err error)

	// BlockchainInitHook is called when the blockchain is initialized.
	BlockchainInitHook = func(chainConfig *params.ChainConfig)

	CloseHook = func()

	// BlockStartHook is called before executing `block`.
	// `td` is the total difficulty prior to `block`.
	BlockStartHook = func(block *types.Block)

	// BlockEndHook is called after executing a block.
	BlockEndHook = func(err error)

	// GenesisBlockHook is called when the genesis block is being processed.
	GenesisBlockHook = func(genesis *types.Block, alloc types.GenesisAlloc)

	// CommitHook is called when the state is committed.
	CommitHook = func(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte)

	// LogHook is called when a log is emitted.
	LogHook = func(log *types.Log)

	OnSystemCallStartHookV2 = func(vm *VMContext)
)

type Hooks struct {
	// VM events
	OnTxStart TxStartHook
	OnTxEnd   TxEndHook
	// Chain events
	OnBlockchainInit        BlockchainInitHook
	OnClose                 CloseHook
	OnBlockStart            BlockStartHook
	OnSystemCallStartHookV2 OnSystemCallStartHookV2
	OnBlockEnd              BlockEndHook
	OnGenesisBlock          GenesisBlockHook
	OnLog                   LogHook
	// custom hook
	OnCommit CommitHook
}
