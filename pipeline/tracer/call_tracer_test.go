package tracer

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	ptypes "github.com/ethereum/go-ethereum/pipeline/types"
)

func TestCallTracerSkipsEmptyCallstackHooks(t *testing.T) {
	tracer := newCallTracerRaw()
	tx := types.NewTx(&types.LegacyTx{Gas: 21_000})

	tracer.OnTxStart(&tracing.VMContext{}, tx, common.Address{})
	tracer.CaptureState(0, vm.SSTORE, 0, 0, nil, nil, 0, nil)
	tracer.OnLog(&types.Log{})
	tracer.OnTxEnd(&types.Receipt{}, nil)
}

func testAddr(b byte) *common.Address {
	a := common.BytesToAddress([]byte{b})
	return &a
}

// newTestCallTracer resets the package-level BlockCtx with a fresh BlockFile
// (dbk keeps block state in the global BlockCtx rather than on the tracer).
func newTestCallTracer() *callTracer {
	BlockCtx = &ExtraInfo{
		BlockFile: &ptypes.BlockFile{
			Events:           make([]ptypes.Event, 0),
			Txs:              make([]ptypes.Transaction, 0),
			Traces:           make([]ptypes.Trace, 0),
			ErrorEvents:      make([]ptypes.Event, 0),
			ErrorTraces:      make([]ptypes.Trace, 0),
			StorageContracts: make([]string, 0),
		},
		ChangeContracts: make(map[common.Address]struct{}),
	}
	t := newCallTracerRaw()
	t.txID = "0xdeadbeef"
	return t
}

func findTraceByTo(t *testing.T, traces []ptypes.Trace, to *common.Address) ptypes.Trace {
	t.Helper()
	want := strings.ToLower(to.Hex())
	for _, tr := range traces {
		if tr.To == want {
			return tr
		}
	}
	t.Fatalf("trace with to=%s not found", want)
	return ptypes.Trace{}
}

// 父调用失败时,自身成功的子孙调用也应进入 ErrorTraces,并标记 "parent call failed"
func TestParentFailedChildrenMovedToErrorTraces(t *testing.T) {
	gc0 := callFrame{Type: vm.CALL, To: testAddr(3), Logs: []ptypes.Event{{Address: "0x03", LogIndex: 6}}}
	gc1 := callFrame{Type: vm.CALL, To: testAddr(4), Error: "out of gas"}
	child0 := callFrame{
		Type: vm.CALL, To: testAddr(2), Error: "execution reverted",
		Calls: []callFrame{gc0, gc1},
		Logs:  []ptypes.Event{{Address: "0x02", LogIndex: 5}},
	}
	child1 := callFrame{Type: vm.CALL, To: testAddr(5), Logs: []ptypes.Event{{Address: "0x05", LogIndex: 7}}}
	root := callFrame{Type: vm.CALL, To: testAddr(1), Calls: []callFrame{child0, child1}}

	tr := newTestCallTracer()
	tr.callstack = []callFrame{root}
	tr.OnTxEnd(nil, nil)

	bf := BlockCtx.BlockFile
	if len(bf.Traces) != 2 {
		t.Fatalf("Traces len = %d, want 2 (root, child1)", len(bf.Traces))
	}
	if got := findTraceByTo(t, bf.Traces, testAddr(1)).Error; got != "" {
		t.Errorf("root Error = %q, want empty", got)
	}
	if got := findTraceByTo(t, bf.Traces, testAddr(5)).Error; got != "" {
		t.Errorf("child1 Error = %q, want empty", got)
	}

	if len(bf.ErrorTraces) != 3 {
		t.Fatalf("ErrorTraces len = %d, want 3 (child0, gc0, gc1)", len(bf.ErrorTraces))
	}
	if got := findTraceByTo(t, bf.ErrorTraces, testAddr(2)).Error; got != "execution reverted" {
		t.Errorf("child0 Error = %q, want own error kept", got)
	}
	if got := findTraceByTo(t, bf.ErrorTraces, testAddr(3)).Error; got != "parent call failed" {
		t.Errorf("gc0 Error = %q, want \"parent call failed\"", got)
	}
	if got := findTraceByTo(t, bf.ErrorTraces, testAddr(4)).Error; got != "out of gas" {
		t.Errorf("gc1 Error = %q, want own error kept", got)
	}

	if len(bf.Events) != 1 || bf.Events[0].LogIndex != 7 {
		t.Errorf("Events = %+v, want only child1's log with LogIndex 7", bf.Events)
	}
	if len(bf.ErrorEvents) != 2 {
		t.Fatalf("ErrorEvents len = %d, want 2 (child0's, gc0's)", len(bf.ErrorEvents))
	}
	for _, e := range bf.ErrorEvents {
		if e.LogIndex != 0 {
			t.Errorf("ErrorEvent LogIndex = %d, want 0", e.LogIndex)
		}
	}
}

// 顶层调用失败时,整棵子树进入 ErrorTraces
func TestTopLevelFailedMarksWholeSubtree(t *testing.T) {
	child := callFrame{Type: vm.CALL, To: testAddr(2), Logs: []ptypes.Event{{Address: "0x02", LogIndex: 3}}}
	root := callFrame{Type: vm.CALL, To: testAddr(1), Error: "execution reverted", Calls: []callFrame{child}}

	tr := newTestCallTracer()
	tr.callstack = []callFrame{root}
	tr.OnTxEnd(nil, nil)

	bf := BlockCtx.BlockFile
	if len(bf.Traces) != 0 {
		t.Fatalf("Traces len = %d, want 0", len(bf.Traces))
	}
	if len(bf.ErrorTraces) != 2 {
		t.Fatalf("ErrorTraces len = %d, want 2", len(bf.ErrorTraces))
	}
	if got := findTraceByTo(t, bf.ErrorTraces, testAddr(1)).Error; got != "execution reverted" {
		t.Errorf("root Error = %q, want own error kept", got)
	}
	if got := findTraceByTo(t, bf.ErrorTraces, testAddr(2)).Error; got != "parent call failed" {
		t.Errorf("child Error = %q, want \"parent call failed\"", got)
	}
	if len(bf.Events) != 0 || len(bf.ErrorEvents) != 1 {
		t.Errorf("Events/ErrorEvents len = %d/%d, want 0/1", len(bf.Events), len(bf.ErrorEvents))
	}
}

// 全部成功时,不产生任何 error trace/event
func TestAllSuccessNoErrorTraces(t *testing.T) {
	gc := callFrame{Type: vm.CALL, To: testAddr(3)}
	child := callFrame{Type: vm.CALL, To: testAddr(2), Calls: []callFrame{gc}, Logs: []ptypes.Event{{Address: "0x02", LogIndex: 1}}}
	root := callFrame{Type: vm.CALL, To: testAddr(1), Calls: []callFrame{child}}

	tr := newTestCallTracer()
	tr.callstack = []callFrame{root}
	tr.OnTxEnd(nil, nil)

	bf := BlockCtx.BlockFile
	if len(bf.Traces) != 3 || len(bf.ErrorTraces) != 0 {
		t.Fatalf("Traces/ErrorTraces len = %d/%d, want 3/0", len(bf.Traces), len(bf.ErrorTraces))
	}
	for _, trace := range bf.Traces {
		if trace.Error != "" {
			t.Errorf("trace to=%s Error = %q, want empty", trace.To, trace.Error)
		}
	}
	if len(bf.Events) != 1 || len(bf.ErrorEvents) != 0 {
		t.Errorf("Events/ErrorEvents len = %d/%d, want 1/0", len(bf.Events), len(bf.ErrorEvents))
	}
}
