package tracer

import (
	"encoding/json"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/debank/types"
)

type CallTracer struct {
	blockFile   *types.BlockFile
	txHash      string
	callStack   []*types.Trace
	traceIdx    int
	eventIdx    int
	stateTracer *StateTracer
}

func NewCallTracer(blockFile *types.BlockFile, txHash string, stateTracer *StateTracer) *CallTracer {
	return &CallTracer{
		blockFile:   blockFile,
		txHash:      txHash,
		callStack:   make([]*types.Trace, 0),
		traceIdx:    0,
		eventIdx:    0,
		stateTracer: stateTracer,
	}
}

func (ct *CallTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	var callType string
	var createType string

	if create {
		createType = "create"
		callType = ""
	} else {
		createType = ""
		if len(input) == 0 {
			callType = "call"
		} else {
			callType = "call"
		}
	}

	traceAddress := make([]int64, 0)
	for i := 0; i < len(ct.callStack); i++ {
		traceAddress = append(traceAddress, ct.callStack[i].Subtraces)
	}

	var parentTraceID string
	var posInParentTrace int64
	if len(ct.callStack) > 0 {
		parent := ct.callStack[len(ct.callStack)-1]
		parentTraceID = parent.ID
		posInParentTrace = parent.Subtraces
		parent.Subtraces++
	} else {
		parentTraceID = ""
		posInParentTrace = 0
	}

	traceID := generateTraceID(ct.txHash, parentTraceID, posInParentTrace)

	var traceValue *hexutil.Big
	if value != nil {
		traceValue = (*hexutil.Big)(value)
	} else {
		traceValue = (*hexutil.Big)(big.NewInt(0))
	}

	trace := &types.Trace{
		ID:                traceID,
		From:              from.Hex(),
		Gas:               big.NewInt(int64(gas)),
		Input:             hexutil.Bytes(input),
		To:                to.Hex(),
		Value:             traceValue,
		GasUsed:           big.NewInt(0),
		Output:            hexutil.Bytes{},
		CallCreateType:    createType,
		CallType:          callType,
		TxID:              ct.txHash,
		ParentTraceID:     parentTraceID,
		PosInParentTrace:  posInParentTrace,
		SelfStorageChange: false,
		StorageChange:     false,
		Subtraces:         0,
		TraceAddress:      traceAddress,
		Error:             "",
	}

	ct.callStack = append(ct.callStack, trace)
	ct.traceIdx++
}

func (ct *CallTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if len(ct.callStack) == 0 {
		return
	}

	trace := ct.callStack[len(ct.callStack)-1]
	ct.callStack = ct.callStack[:len(ct.callStack)-1]

	trace.GasUsed = big.NewInt(int64(gasUsed))
	trace.Output = hexutil.Bytes(output)

	if err != nil {
		trace.Error = err.Error()
		ct.blockFile.ErrorTraces = append(ct.blockFile.ErrorTraces, *trace)
	} else {
		ct.blockFile.Traces = append(ct.blockFile.Traces, *trace)
	}
}

func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	create := typ == vm.CREATE || typ == vm.CREATE2

	var callType string
	if create {
		callType = "create"
	} else {
		switch typ {
		case vm.CALL:
			callType = "call"
		case vm.CALLCODE:
			callType = "callcode"
		case vm.DELEGATECALL:
			callType = "delegatecall"
		case vm.STATICCALL:
			callType = "staticcall"
		default:
			callType = "call"
		}
	}

	traceAddress := make([]int64, 0)
	for i := 0; i < len(ct.callStack); i++ {
		traceAddress = append(traceAddress, ct.callStack[i].Subtraces)
	}

	var parentTraceID string
	var posInParentTrace int64
	if len(ct.callStack) > 0 {
		parent := ct.callStack[len(ct.callStack)-1]
		parentTraceID = parent.ID
		posInParentTrace = parent.Subtraces
		parent.Subtraces++
	}

	traceID := generateTraceID(ct.txHash, parentTraceID, posInParentTrace)

	var traceValue *hexutil.Big
	if value != nil {
		traceValue = (*hexutil.Big)(value)
	} else {
		traceValue = (*hexutil.Big)(big.NewInt(0))
	}

	trace := &types.Trace{
		ID:                traceID,
		From:              from.Hex(),
		Gas:               big.NewInt(int64(gas)),
		Input:             hexutil.Bytes(input),
		To:                to.Hex(),
		Value:             traceValue,
		GasUsed:           big.NewInt(0),
		Output:            hexutil.Bytes{},
		CallCreateType:    "",
		CallType:          callType,
		TxID:              ct.txHash,
		ParentTraceID:     parentTraceID,
		PosInParentTrace:  posInParentTrace,
		SelfStorageChange: false,
		StorageChange:     false,
		Subtraces:         0,
		TraceAddress:      traceAddress,
		Error:             "",
	}

	if create {
		trace.CallCreateType = "create"
		trace.CallType = ""
	}

	ct.callStack = append(ct.callStack, trace)
}

func (ct *CallTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if len(ct.callStack) == 0 {
		return
	}

	trace := ct.callStack[len(ct.callStack)-1]
	ct.callStack = ct.callStack[:len(ct.callStack)-1]

	trace.GasUsed = big.NewInt(int64(gasUsed))
	trace.Output = hexutil.Bytes(output)

	if err != nil {
		trace.Error = err.Error()
		ct.blockFile.ErrorTraces = append(ct.blockFile.ErrorTraces, *trace)
	} else {
		ct.blockFile.Traces = append(ct.blockFile.Traces, *trace)
	}
}

func (ct *CallTracer) CaptureTxStart(gasLimit uint64) {
}

func (ct *CallTracer) CaptureTxEnd(restGas uint64) {
}

func (ct *CallTracer) ProcessLog(log *coretypes.Log, parentTraceID string) {
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}

	var selector string
	if len(topics) > 0 {
		selector = topics[0]
	}

	eventID := generateEventID(parentTraceID, int64(ct.eventIdx))

	event := types.Event{
		ID:            eventID,
		Address:       log.Address.Hex(),
		Selector:      selector,
		Topics:        topics,
		Data:          hexutil.Bytes(log.Data),
		ParentTraceID: parentTraceID,
		Position:      int64(ct.eventIdx),
		LogIndex:      int64(log.Index),
	}

	ct.blockFile.Events = append(ct.blockFile.Events, event)
	ct.eventIdx++
}

func generateTraceID(txID, parentTraceID string, posInParentTrace int64) string {
	data := txID + parentTraceID + strconv.FormatInt(posInParentTrace, 10)
	hash := crypto.Keccak256Hash([]byte(data))
	return hash.Hex()
}

func generateEventID(parentTraceID string, position int64) string {
	data := parentTraceID + strconv.FormatInt(position, 10)
	hash := crypto.Keccak256Hash([]byte(data))
	return hash.Hex()
}

func (ct *CallTracer) GetResult() (json.RawMessage, error) {
	return nil, nil
}

func (ct *CallTracer) Stop(err error) {
}

// UpdateStorageChanges marks traces that caused storage changes
func (ct *CallTracer) UpdateStorageChanges(stateTracer *StateTracer) {
	// Get addresses that had storage changes
	storageContracts := make(map[string]bool)
	for addr := range stateTracer.StorageChanges {
		storageContracts[addr.Hex()] = true
	}

	// Update traces in block file to mark storage changes
	for i := range ct.blockFile.Traces {
		trace := &ct.blockFile.Traces[i]
		if storageContracts[trace.To] {
			trace.StorageChange = true
			// If this trace directly modified storage, mark it as self storage change
			trace.SelfStorageChange = true
		}
	}

	// Update error traces as well
	for i := range ct.blockFile.ErrorTraces {
		trace := &ct.blockFile.ErrorTraces[i]
		if storageContracts[trace.To] {
			trace.StorageChange = true
			trace.SelfStorageChange = true
		}
	}
}
