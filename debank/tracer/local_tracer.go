package tracer

import (
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	ptypes "github.com/ethereum/go-ethereum/debank/types"
	"github.com/ethereum/go-ethereum/debank/util"
	"github.com/ethereum/go-ethereum/log"
)

var _ vm.EVMLogger = (*LocalTracer)(nil)

type LocalTracer struct {
	callTracer   *callTracer
	currentBlock *LocalBlockContext
}

type LocalBlockContext struct {
	BlockDiff       *ptypes.BlockStorageDiff
	BlockHeader     *ptypes.Header
	BlockFile       *ptypes.BlockFile
	Tx              *types.Transaction
	From            common.Address
	ChangeContracts map[common.Address]struct{}
}

func NewLocalTracer() *LocalTracer {
	return &LocalTracer{}
}

func (t *LocalTracer) OnBlockStart(block *types.Block) {
	t.currentBlock = &LocalBlockContext{
		ChangeContracts: make(map[common.Address]struct{}),
	}
	t.currentBlock.BlockDiff = &ptypes.BlockStorageDiff{}
	t.currentBlock.BlockHeader = util.BuildPilelineBlockHeader(block)
	t.currentBlock.BlockFile = &ptypes.BlockFile{
		Block:            util.BuildPipelineBlock(block),
		Events:           make([]ptypes.Event, 0),
		Txs:              make([]ptypes.Transaction, 0),
		Traces:           make([]ptypes.Trace, 0),
		ErrorEvents:      make([]ptypes.Event, 0),
		ErrorTraces:      make([]ptypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
}

func (t *LocalTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureStart(env, from, to, create, input, gas, value)
	}
}

func (t *LocalTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnd(output, gasUsed, err)
	}
}

func (t *LocalTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnter(typ, from, to, input, gas, value)
	}
}

func (t *LocalTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureExit(output, gasUsed, err)
	}
}

func (t *LocalTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureFault(pc, op, gas, cost, scope, depth, err)
	}
}

func (t *LocalTracer) CaptureTxStart(gas uint64) {
}

func (t *LocalTracer) CaptureTxEnd(restGas uint64) {
}

func (t *LocalTracer) OnTxStart(tx *types.Transaction, from common.Address) {
	callTracer := newCallTracerRaw(t.currentBlock.ChangeContracts, t.currentBlock.BlockFile)
	t.callTracer = callTracer
	t.callTracer.OnTxStart(tx, from)
	t.currentBlock.From = from
	t.currentBlock.Tx = tx
}

func (t *LocalTracer) OnTxEnd(receipt *types.Receipt, err error) {
	t.callTracer.OnTxEnd(receipt, err)
	t.callTracer = nil

	tx := util.BuildPipelineTransaction(t.currentBlock.Tx, receipt, t.currentBlock.From, t.currentBlock.BlockHeader.BaseFeePerGas.ToInt())
	t.currentBlock.BlockFile.Txs = append(t.currentBlock.BlockFile.Txs, tx)
}

func (t *LocalTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureState(pc, op, gas, cost, scope, rData, depth, err)
	}
}

func (t *LocalTracer) OnLog(log *types.Log) {
	if t.callTracer != nil {
		t.callTracer.OnLog(log)
	}
}

func (t *LocalTracer) OutPut(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash]types.StateAccount, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte) *ptypes.DebankOutPut {
	if originRoot != root {
		t.currentBlock.BlockDiff = stateUpdateToStateDiff(originRoot, root, destructs, accounts, nil, storages, nil, codes)
	} else {
		t.currentBlock.BlockDiff = nil
	}

	for addr := range t.currentBlock.ChangeContracts {
		t.currentBlock.BlockFile.StorageContracts = append(t.currentBlock.BlockFile.StorageContracts, strings.ToLower(addr.Hex()))
	}

	// Generate DebankOutPut
	var stateDiffBytes []byte
	var err error
	if t.currentBlock.BlockDiff != nil {
		stateDiffBytes, err = util.EncodeToRlp(t.currentBlock.BlockDiff)
		if err != nil {
			log.Error("Failed to encode state diff", "err", err)
			stateDiffBytes = []byte{}
		}
	} else {
		stateDiffBytes = []byte{}
	}

	return &ptypes.DebankOutPut{
		BlockFile:      t.currentBlock.BlockFile,
		Header:         t.currentBlock.BlockHeader,
		StateDiff:      hexutil.Bytes(stateDiffBytes),
		ValidationHash: t.currentBlock.BlockFile.Validation().ValidationHash,
	}
}

func GenesisAllocToStateDiff(genesisAlloc core.GenesisAlloc) *ptypes.BlockStorageDiff {
	diff := &ptypes.BlockStorageDiff{}
	diff.NewAccounts = make([]ptypes.NewAccount, 0)
	diff.NewCodes = make([]ptypes.NewCode, 0)
	diff.StorageDiff = make([]ptypes.AccountStorageDiff, 0)
	diff.DeletedAccounts = make([]common.Hash, 0)
	for addr, acc := range genesisAlloc {
		diff.NewAccounts = append(diff.NewAccounts, ptypes.NewAccount{
			Address:  crypto.Keccak256Hash(addr[:]),
			Balance:  uint256.MustFromBig(acc.Balance),
			Nonce:    acc.Nonce,
			CodeHash: crypto.HashData(crypto.NewKeccakState(), acc.Code),
		})
		if len(acc.Code) > 0 {
			diff.NewCodes = append(diff.NewCodes, ptypes.NewCode{
				CodeHash: crypto.HashData(crypto.NewKeccakState(), acc.Code),
				Code:     acc.Code,
			})
		}
		values := make([]ptypes.IndexValuePair, 0)
		for index, v := range acc.Storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				value = uint256.NewInt(0).SetBytes(v.Bytes())
			}
			values = append(values, ptypes.IndexValuePair{
				Index: crypto.Keccak256Hash(index[:]),
				Value: value,
			})
		}
		diff.StorageDiff = append(diff.StorageDiff, ptypes.AccountStorageDiff{
			Address: crypto.Keccak256Hash(addr[:]),
			Values:  values,
		})
	}
	return diff
}

func stateUpdateToStateDiff(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash]types.StateAccount, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) *ptypes.BlockStorageDiff {
	stateDiff := &ptypes.BlockStorageDiff{}
	for addrhash := range destructs {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addrhash)
	}
	for k, account := range accounts {
		stateDiff.NewAccounts = append(stateDiff.NewAccounts, ptypes.NewAccount{
			Address:  k,
			Balance:  uint256.MustFromBig(account.Balance),
			Nonce:    account.Nonce,
			CodeHash: common.BytesToHash(account.CodeHash),
		})
	}
	for account, storage := range storages {
		Values := make([]ptypes.IndexValuePair, 0, len(storage))
		for index, v := range storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				_, content, _, err := rlp.Split(v)
				if err != nil {
					log.Error("Failed to split storage", "err", err)
				}
				value = uint256.NewInt(0).SetBytes(content)
			}
			Values = append(Values, ptypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		stateDiff.StorageDiff = append(stateDiff.StorageDiff, ptypes.AccountStorageDiff{
			Address: account,
			Values:  Values,
		})
	}
	for hash, code := range codes {
		stateDiff.NewCodes = append(stateDiff.NewCodes, ptypes.NewCode{
			CodeHash: hash,
			Code:     code,
		})
	}
	if originRoot == (common.Hash{}) {
		originRoot = types.EmptyRootHash
	}
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	stateDiff.Hash = root
	stateDiff.ParentHash = originRoot
	return stateDiff
}
