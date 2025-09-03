package tracer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/pipeline/leader"
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
	IsBackup         *bool    `json:"is_backup"` // nil = auto (use etcd), false = leader in fixed mode, true = backup in fixed mode
	EnableStateDiff  bool     `json:"enable_state_diff"`
	Version          string   `json:"version"`

	// Auto failover configurations
	EtcdEndpoints []string `json:"etcd_endpoints"`
	ElectionKey   string   `json:"election_key"`
	NodeID        string   `json:"node_id"`      // default to hostname
	GracePeriod   int      `json:"grace_period"` // default to 10 seconds, unit is second

	// Writer node registry configurations
	WriterRegistryTTL int64 `json:"writer_registry_ttl"` // TTL for writer node registration in seconds, default 30
}

func (config *pipelineTracerConfig) fillDefaultValues() {
	if config.IsBackup == nil && len(config.EtcdEndpoints) == 0 {
		log.Crit("IsBackup is nil and etcd endpoints is empty, please set IsBackup to true(manual mode) or add etcd endpoints(auto mode)")
	}
	if config.IsBackup != nil && len(config.EtcdEndpoints) > 0 {
		log.Crit("IsBackup is not nil and etcd endpoints is not empty, please set IsBackup to nil(auto mode) or remove etcd endpoints(manual mode)")
	}
	if config.NodeID == "" {
		// 先尝试从环境变量 HOSTNAME 读取
		if hostname := os.Getenv("HOSTNAME"); hostname != "" {
			config.NodeID = hostname
		} else {
			// 如果环境变量不存在，则使用系统主机名
			hostname, err := os.Hostname()
			if err != nil {
				log.Crit("Failed to get hostname", "err", err)
			}
			config.NodeID = hostname
		}
	}
	if config.GracePeriod == 0 {
		config.GracePeriod = 10
	}
	// Fill default values for writer registry
	if config.WriterRegistryTTL == 0 {
		config.WriterRegistryTTL = 10 // 10 seconds default TTL
	}
}

func NewPipelineTracer(cfg json.RawMessage) (*PipelineTracer, error) {
	var config pipelineTracerConfig
	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
	}
	config.fillDefaultValues()

	log.Info("NewPipelineTracer", "config", config)

	t := &PipelineTracer{
		config: config,
	}

	return t, nil
}

func (t *PipelineTracer) OnBlockchainInit(chainConfig *params.ChainConfig) {
	log.Info("Init pipeline with param", "chainConfig", chainConfig.ChainID.String(), "config", t.config)

	// set default election key
	if t.config.ElectionKey == "" {
		t.config.ElectionKey = chainConfig.ChainID.String() + "/writers/leader"
	}

	// Initialize pipeline
	err := InitPipeline(t.config.Region, t.config.NodeXBucket, t.config.ChainTableBucket,
		t.config.Brokers, t.config.Topic, chainConfig.ChainID.String(), t.config.Version,
		t.config.S3TempDir)
	if err != nil {
		log.Crit("Failed to init pipeline", "err", err)
	}

	// Prepare writer registry configuration
	var writerConfig *WriterRegistryConfig
	// Writer registry is always enabled when etcd endpoints are configured
	if len(t.config.EtcdEndpoints) > 0 {
		writerConfig = &WriterRegistryConfig{
			TTL:              t.config.WriterRegistryTTL,
			NodeXBucket:      t.config.NodeXBucket,
			ChainTableBucket: t.config.ChainTableBucket,
			Region:           t.config.Region,
			Brokers:          t.config.Brokers,
			Topic:            t.config.Topic,
		}
	}

	// Setup leader election based on configuration
	err = SetupLeaderElection(t.config.EtcdEndpoints, t.config.ElectionKey,
		t.config.NodeID, t.config.IsBackup, t.config.GracePeriod, writerConfig)
	if err != nil {
		log.Crit("Failed to setup leader election", "err", err)
		// Continue without election - will remain in backup mode
	}

	// start upload work should be after leader election
	err = NodeXPusher.StartUploadWork()
	if err != nil {
		log.Crit("Failed to start NodeXPusher upload work", "err", err)
	}
	err = ChainTableBucketPusher.StartUploadWork()
	if err != nil {
		log.Crit("Failed to start ChainTableBucketPusher upload work", "err", err)
	}

	metrics.NodeInfo.Update(map[string]string{
		"chain_id": chainConfig.ChainID.String(),
		"role":     "writer",
	})
}

func (t *PipelineTracer) OnClose() {
	// Unregister writer node if registered
	if WriterRegistry != nil {
		if err := WriterRegistry.UnregisterNode(); err != nil {
			log.Error("Failed to unregister writer node during shutdown", "err", err)
		} else {
			log.Info("Writer node unregistered during shutdown")
		}
	}

	// Close leader manager if it exists
	if LeaderManager != nil {
		LeaderManager.Close()
		// Clear global reference
		leader.GlobalManager = nil
	}
	// Close processors
	if NodeXPusher != nil {
		NodeXPusher.Close()
	}
	if ChainTableBucketPusher != nil {
		ChainTableBucketPusher.Close()
	}
}

func (t *PipelineTracer) OnBlockStart(block *types.Block) {
	BlockCtx = &ExtraInfo{
		BlockNumber: block.Number().Uint64(),
		BlockHash:   block.Hash(),
	}
	BlockCtx.BlockDiff = &ptypes.BlockStorageDiff{}
	BlockCtx.BlockHeader = util.BuildPilelineBlockHeader(block)
	BlockCtx.BlockFile = &ptypes.BlockFile{
		Block:            util.BuildPipelineBlock(block),
		Events:           make([]ptypes.Event, 0),
		Txs:              make([]ptypes.Transaction, 0),
		Traces:           make([]ptypes.Trace, 0),
		ErrorEvents:      make([]ptypes.Event, 0),
		ErrorTraces:      make([]ptypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
	BlockCtx.Tx = nil
	BlockCtx.From = common.Address{}
	BlockCtx.BlockStartTime = time.Now()
	BlockCtx.Committed = false
	BlockCtx.ChangeContracts = make(map[common.Address]struct{})
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
	for addr, account := range alloc {
		if len(account.Storage) > 0 {
			blockFile.StorageContracts = append(blockFile.StorageContracts, strings.ToLower(addr.Hex()))
		}
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
			BlockCtx.BlockDiff = t.prestateTracer.GetStateDiff(originRoot, root)
		}
	} else {
		BlockCtx.BlockDiff = nil
	}

	for addr := range BlockCtx.ChangeContracts {
		BlockCtx.BlockFile.StorageContracts = append(BlockCtx.BlockFile.StorageContracts, strings.ToLower(addr.Hex()))
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

	log.Info("Upload block", "block number", BlockCtx.BlockNumber, "block hash", BlockCtx.BlockHash.Hex())

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
