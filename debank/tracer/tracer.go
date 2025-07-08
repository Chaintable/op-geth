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

// CallTracer is a tracer that captures all call traces, events, and state changes
type CallTracer struct {
	blockFile   *types.BlockFile
	txHash      string
	callStack   []*types.Trace
	traceIdx    int
	eventIdx    int
	stateTracer *StateTracer
}

// NewCallTracer creates a new call tracer
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

// CaptureStart implements vm.EVMLogger
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

	// Calculate trace address
	traceAddress := make([]int64, 0)
	for i := 0; i < len(ct.callStack); i++ {
		traceAddress = append(traceAddress, ct.callStack[i].Subtraces)
	}

	// Generate trace ID
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

	trace := &types.Trace{
		ID:                traceID,
		From:              from.Hex(),
		Gas:               big.NewInt(int64(gas)),
		Input:             hexutil.Bytes(input),
		To:                to.Hex(),
		Value:             (*hexutil.Big)(value),
		GasUsed:           big.NewInt(0), // Will be set in CaptureEnd
		Output:            hexutil.Bytes{},
		CallCreateType:    createType,
		CallType:          callType,
		TxID:              ct.txHash,
		ParentTraceID:     parentTraceID,
		PosInParentTrace:  posInParentTrace,
		SelfStorageChange: false, // Will be updated by state tracer
		StorageChange:     false, // Will be updated by state tracer
		Subtraces:         0,
		TraceAddress:      traceAddress,
		Error:             "",
	}

	ct.callStack = append(ct.callStack, trace)
	ct.traceIdx++
}

// CaptureEnd implements vm.EVMLogger
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
		// Add to error traces
		ct.blockFile.ErrorTraces = append(ct.blockFile.ErrorTraces, *trace)
	} else {
		// Add to normal traces
		ct.blockFile.Traces = append(ct.blockFile.Traces, *trace)
	}
}

// CaptureState implements vm.EVMLogger
func (ct *CallTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// We don't need to capture every opcode execution for our use case
}

// CaptureFault implements vm.EVMLogger
func (ct *CallTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
	// Fault handling if needed
}

// CaptureEnter implements vm.EVMLogger
func (ct *CallTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	// Handle subcalls
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

	// Calculate trace address
	traceAddress := make([]int64, 0)
	for i := 0; i < len(ct.callStack); i++ {
		traceAddress = append(traceAddress, ct.callStack[i].Subtraces)
	}

	// Generate trace ID
	var parentTraceID string
	var posInParentTrace int64
	if len(ct.callStack) > 0 {
		parent := ct.callStack[len(ct.callStack)-1]
		parentTraceID = parent.ID
		posInParentTrace = parent.Subtraces
		parent.Subtraces++
	}

	traceID := generateTraceID(ct.txHash, parentTraceID, posInParentTrace)

	trace := &types.Trace{
		ID:                traceID,
		From:              from.Hex(),
		Gas:               big.NewInt(int64(gas)),
		Input:             hexutil.Bytes(input),
		To:                to.Hex(),
		Value:             (*hexutil.Big)(value),
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

// CaptureExit implements vm.EVMLogger
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

// CaptureTxStart implements vm.EVMLogger
func (ct *CallTracer) CaptureTxStart(gasLimit uint64) {
	// Transaction start
}

// CaptureTxEnd implements vm.EVMLogger
func (ct *CallTracer) CaptureTxEnd(restGas uint64) {
	// Transaction end
}

// ProcessLog processes event logs after transaction execution
func (ct *CallTracer) ProcessLog(log *coretypes.Log, parentTraceID string) {
	// Convert topics to string array
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}

	// Get selector (first topic for non-anonymous events)
	var selector string
	if len(topics) > 0 {
		selector = topics[0]
	}

	// Generate event ID
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

// generateTraceID generates a unique trace ID
func generateTraceID(txID, parentTraceID string, posInParentTrace int64) string {
	data := txID + parentTraceID + strconv.FormatInt(posInParentTrace, 10)
	hash := crypto.Keccak256Hash([]byte(data))
	return hash.Hex()
}

// generateEventID generates a unique event ID
func generateEventID(parentTraceID string, position int64) string {
	data := parentTraceID + strconv.FormatInt(position, 10)
	hash := crypto.Keccak256Hash([]byte(data))
	return hash.Hex()
}

// GetResult returns the final result (not used in our implementation)
func (ct *CallTracer) GetResult() (json.RawMessage, error) {
	return nil, nil
}

// Stop terminates execution of the tracer
func (ct *CallTracer) Stop(err error) {
	// Cleanup if needed
}
