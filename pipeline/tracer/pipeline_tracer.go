package tracer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/pipeline/metrics"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	ptypes "github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/ethereum/go-ethereum/pipeline/util"
)

var _ vm.EVMLogger = (*PipelineTracer)(nil)

// 需要上传3种data
// 1. block
// 2. state diff
// 3. block file

type PipelineTracer struct {
	config         pipelineTracerConfig
	callTracer     *callTracer
	prestateTracer *prestateTracer
}

type pipelineTracerConfig struct {
	Region           string   `json:"region"`
	NodeXBucket      string   `json:"node_x_bucket"`
	ChainTableBucket string   `json:"chain_table_bucket"`
	Brokers          []string `json:"brokers"`
	Topic            string   `json:"topic"`
	S3TempDir        string   `json:"s3_temp_dir"`
	IsBackup         bool     `json:"is_backup"`
	EnableStateDiff  bool     `json:"enable_state_diff"`
}

func NewPipelineTracer(cfg json.RawMessage) (*PipelineTracer, error) {
	var config pipelineTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
	}
	t := &PipelineTracer{
		config: config,
	}

	return t, nil
}

func (t *PipelineTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	log.Info("Init pipeline with param", "chainConfig", chainConfig.ChainID.String(), "config", t.config)
	err := InitPipeline(t.config.Region, t.config.NodeXBucket, t.config.ChainTableBucket, t.config.Brokers, t.config.Topic, chainConfig.ChainID.String(), t.config.S3TempDir, t.config.IsBackup)
	if err != nil {
		log.Crit("Failed to init pipeline", "err", err)
	}
	metrics.NodeInfo.Update(map[string]string{
		"chain_id": chainConfig.ChainID.String(),
		"role":     "writer",
	})
}

func (t *PipelineTracer) OnClose() {
	NodeXPusher.Close()
}

func (t *PipelineTracer) OnBlockStart(block *types.Block) {
	BlockCtx = &ExtraInfo{
		BlockNumber: block.Number().Uint64(),
		BlockHash:   block.Hash(),
	}
	BlockCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	BlockCtx.BlockHeader = util.BuildPilelineBlockHeader(block)
	BlockCtx.BlockFile = &ptypes.BlockFile{
		Block:  util.BuildPipelineBlock(block),
		Events: make([]ptypes.Event, 0),
		Txs:    make([]ptypes.Transaction, 0),
		Traces: make([]ptypes.Trace, 0),
	}
	BlockCtx.Tx = nil
	BlockCtx.From = common.Address{}
	BlockCtx.BlockStartTime = time.Now()
	BlockCtx.Committed = false
	if t.config.EnableStateDiff {
		t.prestateTracer = newPrestateTracer(&prestateTracerConfig{
			DiffMode: true,
		})
	}
}

func (t *PipelineTracer) OnBlockEnd(blockErr error) {
	// empty block process
	if !BlockCtx.Committed {
		t.OnCommit(BlockCtx.BlockHeader.StateRoot, BlockCtx.BlockHeader.StateRoot, nil, nil, nil, nil, nil, nil)
	}

	// push block change notification
	if BlockCtx.BlockChange != nil {
		start := time.Now()
		err := NodeXPusher.PushBlockChangeNotification(BlockCtx.BlockChange)
		if err == nil {
			log.Info("Push kafka", "dropBlocks", BlockCtx.BlockChange.DropBlocks, "newBlocks", BlockCtx.BlockChange.NewBlocks, "kafka elapsed", common.PrettyDuration(time.Since(start)))
		} else {
			log.Error("Failed to push kafka", "err", err, "dropBlocks", BlockCtx.BlockChange.DropBlocks, "newBlocks", BlockCtx.BlockChange.NewBlocks)
		}
	}
	metrics.BlockProcessTimer.UpdateSince(BlockCtx.BlockStartTime)
}

func (t *PipelineTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureStart(env, from, to, create, input, gas, value)
	}
}

func (t *PipelineTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnd(output, gasUsed, err)
	}
}

func (t *PipelineTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	if t.callTracer != nil {
		t.callTracer.CaptureEnter(typ, from, to, input, gas, value)
	}
}

func (t *PipelineTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureExit(output, gasUsed, err)
	}
}

func (t *PipelineTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
	if t.callTracer == nil {
		return
	}
	t.callTracer.CaptureFault(pc, op, gas, cost, scope, depth, err)
}

func (t *PipelineTracer) CaptureTxStart(gas uint64) {
}

func (t *PipelineTracer) CaptureTxEnd(restGas uint64) {
}

func (t *PipelineTracer) OnSystemCallStartHookV2(vmContext *tracing.VMContext) {
	if t.prestateTracer != nil {
		t.prestateTracer.OnSystemCallStartHookV2(vmContext)
	}
}

func (t *PipelineTracer) OnTxStart(vmContext *tracing.VMContext, tx *types.Transaction, from common.Address) {
	callTracer := newCallTracerRaw()
	t.callTracer = callTracer
	t.callTracer.OnTxStart(vmContext, tx, from)
	if t.prestateTracer != nil {
		t.prestateTracer.OnTxStart(vmContext, tx, from)
	}
	BlockCtx.Tx = tx
	BlockCtx.From = from
	BlockCtx.TxStartTime = time.Now()
}

func (t *PipelineTracer) OnTxEnd(receipt *types.Receipt, err error) {
	defer func() {
		metrics.BlockTxExecutionTimer.UpdateSince(BlockCtx.TxStartTime)
	}()
	t.callTracer.OnTxEnd(receipt, err)
	if t.prestateTracer != nil {
		t.prestateTracer.OnTxEnd(receipt, err)
	}
	t.callTracer = nil

	tx := util.BuildPipelineTransaction(BlockCtx.Tx, receipt, BlockCtx.From, BlockCtx.BlockHeader.BaseFeePerGas.ToInt())
	BlockCtx.BlockFile.Txs = append(BlockCtx.BlockFile.Txs, tx)
}

func (t *PipelineTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if t.callTracer != nil {
		t.callTracer.CaptureState(pc, op, gas, cost, scope, rData, depth, err)
	}
	if t.prestateTracer != nil {
		t.prestateTracer.CaptureState(pc, op, gas, cost, scope, rData, depth, err)
	}
}

func (t *PipelineTracer) OnLog(log *types.Log) {
	if t.callTracer != nil {
		t.callTracer.OnLog(log)
	}
}

func (t *PipelineTracer) OnGenesisBlock(block *types.Block, alloc types.GenesisAlloc) {
	if NodeXPusher.LastBlockNotice != nil {
		return
	}

	// 内部s3
	header := util.BuildPilelineBlockHeader(block)
	err := uploadBlockHeader(header)
	if err != nil {
		log.Crit("Failed to upload block", "err", err)
	}
	log.Info("[inner s3] 1.upload genesis block", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())

	blockDiff := GenesisAllocToStateDiff(alloc)
	blockDiff.Hash = block.Root()
	// genesis block has no parent
	blockDiff.ParentHash = types.EmptyRootHash
	err = uploadBlockDiff(blockDiff)
	if err != nil {
		log.Crit("Failed to upload block diff files to s3", "err", err)
	}
	log.Info("[inner s3] 2.upload genesis state diff", "block", block.Hash().Hex())

	// 业务s3
	blockFile := &ptypes.BlockFile{
		Block: util.BuildPipelineBlock(block),
	}
	// upload block file and meta data
	err = uploadBlockFile(blockFile)
	if err != nil {
		log.Crit("Failed to upload block files to s3", "err", err)
	}
	log.Info("3.upload block file", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// upload block file validation
	err = uploadblockFileValidation(blockFile)
	if err != nil {
		log.Crit("Failed to upload file validation to s3", "err", err)
	}
	log.Info("4.upload block file validation", "block hash", header.Hash.Hex(), "block number", header.Number.ToInt().Uint64())

	// push block change notification
	blockChanges := &ptypes.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []ptypes.BlockContext{
			{
				Hash:        block.Hash(),
				ParentHash:  block.ParentHash(),
				BlockNumber: block.NumberU64(),
				Timestamp:   block.Time(),
			},
		},
	}

	err = NodeXPusher.PushBlockChangeNotification(blockChanges)
	if err != nil {
		log.Crit("Failed to push block change notification", "err", err)
	}

	log.Info("push genesis block change notification", "block hash", block.Hash().Hex(), "block number", block.Number().Uint64())
}

func (t *PipelineTracer) OnCommit(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) {
	if originRoot != root {
		if !t.config.EnableStateDiff {
			BlockCtx.BlockDiff = stateUpdateToStateDiff(originRoot, root, destructs, accounts, accountsOrigin, storages, storagesOrigin, codes)
		} else {
			stateDiffA := stateUpdateToStateDiff(originRoot, root, destructs, accounts, accountsOrigin, storages, storagesOrigin, codes)
			stateDiffB := t.prestateTracer.GetStateDiff(originRoot, root)
			if !stateDiffA.Equal(stateDiffB) {
				log.Crit("State diff mismatch", "originRoot", originRoot.Hex(), "root", root.Hex(), "stateDiffA", stateDiffA, "stateDiffB", stateDiffB)
			}
			BlockCtx.BlockDiff = stateDiffB
		}
	} else {
		BlockCtx.BlockDiff = nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploadErrs []error

	// Helper function to handle errors safely
	handleError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			uploadErrs = append(uploadErrs, err)
		}
	}

	// 上传 block head
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadBlockHeader(BlockCtx.BlockHeader)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 state diff
	wg.Add(1)
	go func() {
		defer wg.Done()
		if BlockCtx.BlockDiff == nil {
			return
		}
		err := uploadBlockDiff(BlockCtx.BlockDiff)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadBlockFile(BlockCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 上传 block file validation
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := uploadblockFileValidation(BlockCtx.BlockFile)
		if err != nil {
			handleError(err)
			return
		}
	}()

	// 等待所有上传完成
	wg.Wait()

	if t.config.IsBackup {
		log.Info("Backup Upload block", "block number", BlockCtx.BlockNumber, "block hash", BlockCtx.BlockHash.Hex())
	} else {
		log.Info("Upload block", "block number", BlockCtx.BlockNumber, "block hash", BlockCtx.BlockHash.Hex())
	}

	// 检查是否有错误
	if len(uploadErrs) > 0 {
		for _, err := range uploadErrs {
			log.Error("Upload error", "err", err)
		}
		log.Crit("One or more uploads failed")
	}

	BlockCtx.Committed = true

	metrics.LatestUploadedBlockNumber.Update(int64(BlockCtx.BlockNumber))
}

func BuildHooks(t *PipelineTracer) *tracing.Hooks {
	return &tracing.Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		OnClose:          t.OnClose,
		OnBlockStart:     t.OnBlockStart,
		OnTxStart:        t.OnTxStart,
		OnTxEnd:          t.OnTxEnd,
		OnLog:            t.OnLog,
		OnGenesisBlock:   t.OnGenesisBlock,
		OnCommit:         t.OnCommit,
	}
}

func InitHooks(cfg json.RawMessage) (*tracing.Hooks, error) {
	t, err := NewPipelineTracer(cfg)
	if err != nil {
		return nil, err
	}

	GlobalHooks = &tracing.Hooks{
		OnBlockchainInit: t.OnBlockchainInit,
		OnClose:          t.OnClose,
		OnBlockStart:     t.OnBlockStart,
		OnTxStart:        t.OnTxStart,
		OnTxEnd:          t.OnTxEnd,
		OnLog:            t.OnLog,
		OnGenesisBlock:   t.OnGenesisBlock,
		OnCommit:         t.OnCommit,
	}
	return GlobalHooks, nil
}

func GetHooks() *tracing.Hooks {
	return GlobalHooks
}
