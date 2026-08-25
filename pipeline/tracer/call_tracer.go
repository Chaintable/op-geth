package tracer

// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	ptypes "github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/ethereum/go-ethereum/pipeline/util"
)

var _ vm.EVMLogger = (*callTracer)(nil)

type callFrame struct {
	Type         vm.OpCode       `json:"-"`
	From         common.Address  `json:"from"`
	Gas          uint64          `json:"gas"`
	GasUsed      uint64          `json:"gasUsed"`
	To           *common.Address `json:"to,omitempty" rlp:"optional"`
	Input        []byte          `json:"input" rlp:"optional"`
	Output       []byte          `json:"output,omitempty" rlp:"optional"`
	Error        string          `json:"error,omitempty" rlp:"optional"`
	RevertReason string          `json:"revertReason,omitempty"`
	ParentFailed bool            `json:"-"` // Indicates if the parent call failed
	Calls        []callFrame     `json:"calls,omitempty" rlp:"optional"`
	Logs         []ptypes.Event  `json:"logs,omitempty" rlp:"optional"`

	PosInParentTrace  int    `json:"pos_in_parent_trace"`
	ParentTraceID     string `json:"parent_trace_id"`
	TraceID           string `json:"trace_id"`
	StorageChange     bool   `json:"storageChange"`
	SelfStorageChange bool   `json:"self_storage_change"`

	// Placed at end on purpose. The RLP will be decoded to 0 instead of
	// nil if there are non-empty elements after in the struct.
	Value *big.Int `json:"value,omitempty" rlp:"optional"`
}

func (f callFrame) TypeString() string {
	return f.Type.String()
}

func (f callFrame) failed() bool {
	return len(f.Error) > 0
}

func (f *callFrame) processOutput(output []byte, err error) {
	output = common.CopyBytes(output)
	if err == nil {
		f.Output = output
		return
	}
	f.Error = err.Error()
	if f.Type == vm.CREATE || f.Type == vm.CREATE2 {
		f.To = nil
	}
	if !errors.Is(err, vm.ErrExecutionReverted) || len(output) == 0 {
		return
	}
	f.Output = output
	if len(output) < 4 {
		return
	}
	if unpacked, err := abi.UnpackRevert(output); err == nil {
		f.RevertReason = unpacked
	}
}

type callTracer struct {
	callstack []callFrame
	config    callTracerConfig
	gasLimit  uint64
	interrupt atomic.Bool // Atomic flag to signal execution interruption
	reason    error       // Textual reason for the interruption

	txID string
}

type callTracerConfig struct {
	OnlyTopCall bool `json:"onlyTopCall"` // If true, call tracer won't collect any subcalls
	WithLog     bool `json:"withLog"`     // If true, call tracer will collect event logs
}

func newCallTracerRaw() *callTracer {
	t := &callTracer{callstack: make([]callFrame, 0, 1), config: callTracerConfig{
		OnlyTopCall: false,
		WithLog:     true,
	}}
	return t
}

func (t *callTracer) ToTrace(f *callFrame, traceAddress []int64) ptypes.Trace {
	CallCreateType := ""
	CallType := ""
	switch f.Type {
	case vm.CREATE, vm.CREATE2:
		CallCreateType = strings.ToLower(vm.CREATE.String())
	case vm.SELFDESTRUCT:
		CallCreateType = "suicide"
	case vm.CALL, vm.STATICCALL, vm.CALLCODE, vm.DELEGATECALL:
		CallCreateType = strings.ToLower(vm.CALL.String())
		CallType = strings.ToLower(f.Type.String())
	default:
		CallCreateType = "empty"
	}
	to := common.Address{}
	if f.To != nil {
		to = *f.To
	}
	value := big.NewInt(0)
	if f.Value != nil {
		value = f.Value
	}
	err := ""
	if f.failed() {
		err = f.Error
		if f.RevertReason != "" {
			err = fmt.Sprintf("%s: %s", f.Error, f.RevertReason)
		}
	} else if f.ParentFailed {
		err = "parent call failed"
	}
	return ptypes.Trace{
		ID:                f.TraceID,
		From:              strings.ToLower(f.From.Hex()),
		Gas:               big.NewInt(int64(f.Gas)),
		Input:             (hexutil.Bytes)(f.Input),
		To:                strings.ToLower(to.Hex()),
		Value:             (*hexutil.Big)(value),
		GasUsed:           big.NewInt(int64(f.GasUsed)),
		Output:            (hexutil.Bytes)(f.Output),
		CallCreateType:    CallCreateType,
		CallType:          CallType,
		TxID:              t.txID,
		ParentTraceID:     f.ParentTraceID,
		PosInParentTrace:  int64(f.PosInParentTrace),
		SelfStorageChange: f.SelfStorageChange,
		StorageChange:     f.StorageChange,
		Subtraces:         int64(len(f.Calls)),
		TraceAddress:      traceAddress,
		Error:             err,
	}
}

func (t *callTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if len(t.callstack) == 0 {
		return
	}
	if op == vm.SSTORE {
		t.callstack[len(t.callstack)-1].SelfStorageChange = true
		t.callstack[len(t.callstack)-1].StorageChange = true
	}
}

func (t *callTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	toCopy := to
	callType := vm.CALL
	if create {
		callType = vm.CREATE
	}
	call := callFrame{
		Type:  callType,
		From:  from,
		To:    &toCopy,
		Input: common.CopyBytes(input),
		Gas:   t.gasLimit,
		Value: value,
	}
	t.callstack = append(t.callstack, call)
}

func (t *callTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if len(t.callstack) != 1 {
		return
	}
	t.callstack[0].GasUsed = gasUsed
	t.callstack[0].processOutput(output, err)
}

func (t *callTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.config.OnlyTopCall {
		return
	}
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}

	toCopy := to
	call := callFrame{
		Type:  vm.OpCode(typ),
		From:  from,
		To:    &toCopy,
		Input: common.CopyBytes(input),
		Gas:   gas,
		Value: value,
	}
	t.callstack = append(t.callstack, call)
}

// OnExit is called when EVM exits a scope, even if the scope didn't
// execute any code.
func (t *callTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if t.config.OnlyTopCall {
		return
	}

	size := len(t.callstack)
	if size <= 1 {
		return
	}
	// Pop call.
	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	size -= 1

	call.GasUsed = gasUsed
	call.processOutput(output, err)
	// Nest call into parent.
	call.PosInParentTrace = len(t.callstack[size-1].Calls) + len(t.callstack[size-1].Logs)
	t.callstack[size-1].Calls = append(t.callstack[size-1].Calls, call)
}
func (*callTracer) CaptureTxStart(gasLimit uint64) {
}

func (*callTracer) CaptureTxEnd(restGas uint64) {
}

func (t *callTracer) OnTxStart(vmContext *tracing.VMContext, tx *types.Transaction, from common.Address) {
	t.gasLimit = tx.Gas()
	t.txID = tx.Hash().Hex()
}

func (t *callTracer) OnTxEnd(receipt *types.Receipt, err error) {
	// Error happened during tx validation.
	if err != nil {
		return
	}
	if len(t.callstack) == 0 {
		return
	}
	setParentFailed(&t.callstack[0], false)
	setStorageChange(&t.callstack[0])
	if len(t.callstack) == 1 {
		topCall := &t.callstack[0]
		topCall.TraceID = util.ToHash([]string{t.txID, "", "0"})
		if topCall.failed() {
			BlockCtx.BlockFile.ErrorTraces = append(BlockCtx.BlockFile.ErrorTraces, t.ToTrace(topCall, []int64{}))
		} else {
			BlockCtx.BlockFile.Traces = append(BlockCtx.BlockFile.Traces, t.ToTrace(topCall, []int64{}))
		}
		t.addTraceAndLog(topCall, []int64{})
	}
}

func (t *callTracer) OnLog(log *types.Log) {
	// Skip if tracing was interrupted
	if t.interrupt.Load() {
		return
	}
	if len(t.callstack) == 0 {
		return
	}
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}
	var selector string
	var remainingTopics []string

	if len(topics) > 0 {
		selector = topics[0]
		remainingTopics = topics[1:]
	}
	l := ptypes.Event{
		Address:  strings.ToLower(log.Address.Hex()),
		Selector: selector,
		Topics:   remainingTopics,
		Data:     log.Data,
		Position: int64(len(t.callstack[len(t.callstack)-1].Calls) + len(t.callstack[len(t.callstack)-1].Logs)),
		LogIndex: int64(log.Index),
	}
	t.callstack[len(t.callstack)-1].Logs = append(t.callstack[len(t.callstack)-1].Logs, l)
}

func (t *callTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *callTracer) GetResult() (json.RawMessage, error) {
	return nil, nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *callTracer) Stop(err error) {
	t.reason = err
	t.interrupt.Store(true)
}

// setParentFailed recursively sets the ParentFailed flag for the call frame and all its subcalls.
func setParentFailed(cf *callFrame, parentFailed bool) {
	failed := cf.failed() || parentFailed
	for i := range cf.Calls {
		cf.Calls[i].ParentFailed = failed
		setParentFailed(&cf.Calls[i], failed)
	}
}

func setStorageChange(cf *callFrame) {
	if cf.To != nil && cf.SelfStorageChange {
		if cf.Type == vm.DELEGATECALL {
			BlockCtx.ChangeContracts[cf.From] = struct{}{}
		} else {
			BlockCtx.ChangeContracts[*cf.To] = struct{}{}
		}
	}
	subCallStorageChange := false
	for i := range cf.Calls {
		setStorageChange(&cf.Calls[i])
		if cf.Calls[i].StorageChange && !cf.Calls[i].failed() {
			subCallStorageChange = true
		}
	}
	if subCallStorageChange {
		cf.StorageChange = true
	}
}

func (t *callTracer) addTraceAndLog(cf *callFrame, traceAddress []int64) {
	for i := range cf.Calls {
		cf.Calls[i].ParentTraceID = cf.TraceID
		cf.Calls[i].TraceID = util.ToHash([]string{t.txID, cf.TraceID, fmt.Sprintf("%d", cf.Calls[i].PosInParentTrace)})
		t.addTraceAndLog(&cf.Calls[i], childTraceAddress(traceAddress, int64(i)))
	}
	for i := range cf.Logs {
		cf.Logs[i].ParentTraceID = cf.TraceID
		cf.Logs[i].ID = util.ToHash([]string{cf.Logs[i].ParentTraceID, fmt.Sprintf("%d", cf.Logs[i].Position)})
		if cf.failed() || cf.ParentFailed {
			cf.Logs[i].LogIndex = 0
			BlockCtx.BlockFile.ErrorEvents = append(BlockCtx.BlockFile.ErrorEvents, cf.Logs[i])
		} else {
			BlockCtx.BlockFile.Events = append(BlockCtx.BlockFile.Events, cf.Logs[i])
		}
	}
	for i := range cf.Calls {
		if cf.Calls[i].failed() || cf.Calls[i].ParentFailed {
			BlockCtx.BlockFile.ErrorTraces = append(BlockCtx.BlockFile.ErrorTraces, t.ToTrace(&cf.Calls[i], childTraceAddress(traceAddress, int64(i))))
		} else {
			BlockCtx.BlockFile.Traces = append(BlockCtx.BlockFile.Traces, t.ToTrace(&cf.Calls[i], childTraceAddress(traceAddress, int64(i))))
		}
	}
}

func childTraceAddress(a []int64, i int64) []int64 {
	child := make([]int64, 0, len(a)+1)
	child = append(child, a...)
	child = append(child, i)
	return child
}
