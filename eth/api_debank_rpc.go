package eth

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/internal/ethapi/override"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	singleCallTimeout = 5 * time.Second
	multiCallLimit    = 50
)

type DebankRpcAPI struct {
	backend ethapi.Backend
}

func NewDebankRpcAPI(eth *Ethereum) *DebankRpcAPI {
	return &DebankRpcAPI{
		backend: eth.APIBackend,
	}
}

// EstimateGas implements debank_estimateGas.
func (api *DebankRpcAPI) EstimateGas(
	ctx context.Context,
	args ethapi.TransactionArgs,
	blockContext *DebankBlockContext,
	_ *BlockOverrides,
) (hexutil.Uint64, error) {
	log.Debug("debank_estimateGas")
	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	if blockContext != nil {
		blockNrOrHash = blockContext.GetBlockNumberOrHash()
	}
	return ethapi.DoEstimateGas(ctx, api.backend, args, blockNrOrHash, nil, nil, api.backend.RPCGasCap())
}

// ContractMultiCall implements debank_contractMultiCall.
func (api *DebankRpcAPI) ContractMultiCall(
	ctx context.Context,
	args []ethapi.TransactionArgs,
	blockContext *DebankBlockContext,
	_ *BlockOverrides,
	_ *override.StateOverride,
	pfastFail, puseParallel, pdisableCache *bool,
) (*DebankMultiCallResp, error) {
	log.Debug("debank_contractMultiCall", "count", len(args))

	if len(args) > multiCallLimit {
		return nil, fmt.Errorf("calls exceed limit, expected: <%v, actual: %v", multiCallLimit, len(args))
	}

	setb := func(p *bool, d bool) bool {
		if p == nil {
			return d
		}
		return *p
	}
	fastFail := setb(pfastFail, true)
	useParallel := setb(puseParallel, true)
	disableCache := setb(pdisableCache, false)

	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	if blockContext != nil {
		blockNrOrHash = blockContext.GetBlockNumberOrHash()
	}

	statedb, header, err := api.backend.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if statedb == nil || err != nil {
		return nil, err
	}

	ret := make([]*DebankSingleCallResult, len(args))
	stats := &DebankMultiCallStats{
		BlockNum:     header.Number.Uint64(),
		BlockHash:    header.Hash(),
		BlockTime:    header.Time,
		Success:      true,
		CacheEnabled: !disableCache,
	}

	ctx, cancel := context.WithTimeout(ctx, singleCallTimeout)
	defer cancel()

	if useParallel {
		var wg sync.WaitGroup
		for i, arg := range args {
			wg.Add(1)
			go func(i int, arg ethapi.TransactionArgs) {
				defer wg.Done()
				copied := statedb.Copy()
				r := doOneCall(ctx, api.backend, arg, copied, blockNrOrHash)
				ret[i] = r
				if r.Err != "" {
					stats.Success = false
					if fastFail {
						cancel()
					}
				}
			}(i, arg)
		}
		wg.Wait()
		return &DebankMultiCallResp{Results: ret, Stats: stats}, nil
	}

	failedOnce := false
	for i, arg := range args {
		if failedOnce {
			ret[i] = &DebankSingleCallResult{}
			continue
		}
		r := doOneCall(ctx, api.backend, arg, statedb, blockNrOrHash)
		ret[i] = r
		if r.Err != "" {
			stats.Success = false
			if fastFail {
				failedOnce = true
			}
		}
	}
	return &DebankMultiCallResp{Results: ret, Stats: stats}, nil
}

func doOneCall(
	ctx context.Context,
	b ethapi.Backend,
	arg ethapi.TransactionArgs,
	statedb *state.StateDB,
	blockNrOrHash rpc.BlockNumberOrHash,
) *DebankSingleCallResult {
	result := &DebankSingleCallResult{}
	start := time.Now()
	defer func() {
		result.TimeCost = time.Since(start).Seconds()
	}()

	if arg.To != nil && strings.ToLower(arg.To.Hex()) == nativeAddr {
		var data []byte
		if arg.Input != nil {
			data = *arg.Input
		} else if arg.Data != nil {
			data = *arg.Data
		}
		res, code, err := handleNative(statedb, data)
		if err != nil {
			result.Code = code
			result.Err = err.Error()
		}
		result.Result = res
		return result
	}

	execResult, err := ethapi.DoCall(
		ctx, b, arg, blockNrOrHash, nil, nil,
		b.RPCEVMTimeout(), b.RPCGasCap(),
	)
	if err != nil {
		result.Code = errMessageExecuting
		result.Err = err.Error()
		return result
	}
	if execResult.Err != nil {
		result.Code = errMessageExecuting
		result.Err = execResult.Err.Error()
		return result
	}
	result.Result = execResult.Return()
	result.GasUsed = int64(execResult.UsedGas)
	return result
}

// SimulateTransactions implements debank_simulateTransactions.
func (api *DebankRpcAPI) SimulateTransactions(
	ctx context.Context,
	args []ethapi.TransactionArgs,
	blockContext *DebankBlockContext,
	blockOverrides *override.BlockOverrides,
) (*DebankSimulateResp, error) {
	log.Debug("debank_simulateTransactions", "count", len(args))

	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	if blockContext != nil {
		blockNrOrHash = blockContext.GetBlockNumberOrHash()
	}

	statedb, header, err := api.backend.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if statedb == nil || err != nil {
		return nil, err
	}

	chainConfig := api.backend.ChainConfig()
	blockCtx := core.NewEVMBlockContext(header, ethapi.NewChainContext(ctx, api.backend), nil, chainConfig, statedb)
	if blockOverrides != nil {
		blockOverrides.Apply(&blockCtx)
	}

	stats := DebankSimulateStats{
		BlockNum:  header.Number.Uint64(),
		BlockHash: header.Hash(),
		BlockTime: header.Time,
		Success:   true,
	}

	results := make([]DebankSingleSimulateResult, 0, len(args))

	for i, arg := range args {
		if len(results) > 0 && results[len(results)-1].Code != 0 {
			results = append(results, results[len(results)-1])
			continue
		}

		txHash := common.BigToHash(big.NewInt(int64(i + 1)))

		tracerCfg := json.RawMessage(`{"withLog":true}`)
		tracer, err := tracers.DefaultDirectory.New("callTracer", &tracers.Context{
			BlockHash:   header.Hash(),
			BlockNumber: header.Number,
			TxIndex:     i,
			TxHash:      txHash,
		}, tracerCfg, chainConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create tracer: %w", err)
		}

		if err := arg.CallDefaults(api.backend.RPCGasCap(), header.BaseFee, chainConfig.ChainID); err != nil {
			results = append(results, DebankSingleSimulateResult{
				Traces: []DebankTrace{},
				Events: []DebankEvent{},
				Code:   -39004,
				Err:    err.Error(),
			})
			stats.Success = false
			continue
		}
		msg := arg.ToMessage(header.BaseFee, true, true)

		evm := api.backend.GetEVM(ctx, statedb, header, &vm.Config{
			NoBaseFee: true,
			Tracer:    tracer.Hooks,
		}, &blockCtx)

		gp := new(core.GasPool).AddGas(math.MaxUint64)
		statedb.SetTxContext(txHash, i)
		execResult, execErr := core.ApplyMessage(evm, msg, gp)

		traceResult, traceErr := tracer.GetResult()

		preRes := buildSimulateResult(txHash, execErr, execResult, traceResult, traceErr)
		if preRes.Code != 0 {
			stats.Success = false
		}
		results = append(results, preRes)
	}

	return &DebankSimulateResp{Results: results, Stats: stats}, nil
}

// --- callFrame tree parsing and conversion ---

type callFrameJSON struct {
	Type         string          `json:"type"`
	From         common.Address  `json:"from"`
	To           *common.Address `json:"to,omitempty"`
	Gas          hexutil.Uint64  `json:"gas"`
	GasUsed      hexutil.Uint64  `json:"gasUsed"`
	Input        hexutil.Bytes   `json:"input"`
	Output       hexutil.Bytes   `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	RevertReason string          `json:"revertReason,omitempty"`
	Value        *hexutil.Big    `json:"value,omitempty"`
	Calls        []callFrameJSON `json:"calls,omitempty"`
	Logs         []callLogJSON   `json:"logs,omitempty"`
}

type callLogJSON struct {
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	Position hexutil.Uint   `json:"position"`
}

func calculateDebankID(parts ...string) string {
	h := md5.New()
	for _, p := range parts {
		h.Write([]byte(p))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func walkCallFrame(
	txID common.Hash,
	parentTraceID string,
	posInParent int,
	frame *callFrameJSON,
	traces *[]DebankTrace,
	events *[]DebankEvent,
) {
	trace := DebankTrace{
		TxID:             txID,
		FromAddr:         frame.From,
		GasLimit:         uint64(frame.Gas),
		GasUsed:          uint64(frame.GasUsed),
		Input:            hexutil.Bytes(frame.Input),
		Output:           hexutil.Bytes(frame.Output),
		ParentTraceID:    parentTraceID,
		PosInParentTrace: posInParent,
	}
	if frame.To != nil {
		trace.ToAddr = *frame.To
	}
	if frame.Value != nil {
		trace.Value = frame.Value
	} else {
		trace.Value = (*hexutil.Big)(big.NewInt(0))
	}

	typ := strings.ToUpper(frame.Type)
	switch {
	case typ == "CALL" || typ == "STATICCALL" || typ == "DELEGATECALL" || typ == "CALLCODE":
		trace.CallCreateType = "call"
		trace.CallType = strings.ToLower(typ)
	case typ == "CREATE" || typ == "CREATE2":
		trace.CallCreateType = "create"
	case typ == "SELFDESTRUCT":
		trace.CallCreateType = "suicide"
	default:
		trace.CallCreateType = "call"
		trace.CallType = strings.ToLower(typ)
	}

	trace.ID = calculateDebankID(
		txID.String(),
		trace.ParentTraceID,
		fmt.Sprintf("%d", trace.PosInParentTrace),
	)

	*traces = append(*traces, trace)
	currentTraceID := trace.ID

	sortedLogs := make([]callLogJSON, len(frame.Logs))
	copy(sortedLogs, frame.Logs)
	sort.Slice(sortedLogs, func(i, j int) bool {
		return uint(sortedLogs[i].Position) < uint(sortedLogs[j].Position)
	})

	callIdx := 0
	logIdx := 0
	childPos := 0

	for callIdx < len(frame.Calls) || logIdx < len(sortedLogs) {
		for logIdx < len(sortedLogs) && uint(sortedLogs[logIdx].Position) <= uint(callIdx) {
			appendEvent(sortedLogs[logIdx], txID, currentTraceID, childPos, events)
			childPos++
			logIdx++
		}
		if callIdx < len(frame.Calls) {
			walkCallFrame(txID, currentTraceID, childPos, &frame.Calls[callIdx], traces, events)
			childPos++
			callIdx++
		}
	}

	for logIdx < len(sortedLogs) {
		appendEvent(sortedLogs[logIdx], txID, currentTraceID, childPos, events)
		childPos++
		logIdx++
	}
}

func appendEvent(l callLogJSON, txID common.Hash, parentTraceID string, pos int, events *[]DebankEvent) {
	event := DebankEvent{
		ContractID:       l.Address,
		Data:             hexutil.Bytes(l.Data),
		TxID:             txID,
		ParentTraceID:    parentTraceID,
		PosInParentTrace: pos,
	}
	if len(l.Topics) > 0 {
		event.Selector = l.Topics[0].String()
	}
	if len(l.Topics) > 1 {
		for _, t := range l.Topics[1:] {
			event.Topics = append(event.Topics, t.String())
		}
	}
	event.ID = calculateDebankID(
		event.ParentTraceID,
		fmt.Sprintf("%d", event.PosInParentTrace),
	)
	*events = append(*events, event)
}

func buildSimulateResult(
	txHash common.Hash,
	execErr error,
	execResult *core.ExecutionResult,
	traceResult json.RawMessage,
	traceErr error,
) DebankSingleSimulateResult {
	preRes := DebankSingleSimulateResult{
		Traces: []DebankTrace{},
		Events: []DebankEvent{},
	}

	if execErr != nil {
		preRes.Code = -39004
		preRes.Err = execErr.Error()
		if execResult != nil {
			preRes.GasUsed = execResult.UsedGas
		}
		return preRes
	}

	if execResult != nil {
		preRes.GasUsed = execResult.UsedGas
		if execResult.Failed() {
			if execResult.Err != nil {
				if strings.HasPrefix(execResult.Err.Error(), "execution reverted") {
					preRes.Code = -39000
					reason, _ := abi.UnpackRevert(execResult.Revert())
					if reason != "" {
						preRes.Err = reason
					} else {
						preRes.Err = "execution revert"
					}
				} else {
					preRes.Code = -39004
					preRes.Err = execResult.Err.Error()
				}
			}
		}
	}

	if traceResult != nil && traceErr == nil {
		var root callFrameJSON
		if err := json.Unmarshal(traceResult, &root); err == nil {
			walkCallFrame(txHash, "", 0, &root, &preRes.Traces, &preRes.Events)
		}
	}

	return preRes
}
