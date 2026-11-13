package processor

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/pipeline/failover"
	"github.com/ethereum/go-ethereum/pipeline/metrics"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/ethereum/go-ethereum/pipeline/util"
	"github.com/segmentio/kafka-go"
)

// PushProcessor is a processor that pushes data to s3 and kafka
type PushProcessor struct {
	Bucket          string
	Uploader        *s3.Client
	KafkaWriter     *kafka.Writer
	LastBlockNotice *types.BlockChangeNotification
	S3TempDir       string
	quitCh          chan struct{}
	S3DataCh        chan *DataFile
	Brokers         []string
	Topic           string
	noticeMu        sync.RWMutex
	// lastBlockReloadAt 是最近一次因 LastBlockNotice 为空而重新读取 Kafka 的时间（unix 秒），用于限频
	lastBlockReloadAt atomic.Int64
}

// lastBlockReloadInterval 是 LastBlockNotice 为空时重新读取 Kafka 的最小间隔（秒）
const lastBlockReloadInterval = 10

func NewPushProcessor(region string, bucket string, brokers []string, topic string, s3TempDir string) (*PushProcessor, error) {
	kafkaWriter := util.NewKafkaWriter(brokers, topic)
	s3Uploader, err := util.NewS3Client(region)
	if err != nil {
		return nil, err
	}

	if s3TempDir != "" {
		s3TempDir = filepath.Join(s3TempDir, bucket)
	}

	pusher := &PushProcessor{
		Bucket:      bucket,
		Uploader:    s3Uploader,
		KafkaWriter: kafkaWriter,
		S3TempDir:   s3TempDir,
		quitCh:      make(chan struct{}),
		S3DataCh:    make(chan *DataFile, 100),
		Brokers:     brokers,
		Topic:       topic,
	}

	return pusher, nil
}

func (p *PushProcessor) UpdateLastBlock() error {
	kafkaReader := util.NewKafkaReader(p.Brokers, p.Topic, "")
	defer kafkaReader.Close()

	lastBlockNotice, err := util.GetLastBlockNotice(kafkaReader)
	if err != nil {
		return err
	}
	log.Printf("update last block notice: %+v\n", lastBlockNotice)

	p.noticeMu.Lock()
	p.LastBlockNotice = lastBlockNotice
	p.noticeMu.Unlock()
	return nil
}

func (p *PushProcessor) StartUploadWork() error {
	if p.S3TempDir != "" {
		return p.uploadWork()
	}
	return nil
}

func (p *PushProcessor) uploadWork() error {
	// check p.S3TempDir is exist, create if not exist
	if _, err := os.Stat(p.S3TempDir); os.IsNotExist(err) {
		err = os.MkdirAll(p.S3TempDir, 0755)
		if err != nil {
			log.Printf("failed to create dir: %v", err)
			return err
		}
	}

	files, err := os.ReadDir(p.S3TempDir)
	if err != nil {
		log.Printf("failed to read dir: %v", err)
		return nil
	}
	for _, file := range files {
		// 如果是文件夹，跳过
		if file.IsDir() {
			continue
		}

		fullPath := filepath.Join(p.S3TempDir, file.Name())

		// 读取文件内容
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return err
		}

		// replace - to /
		s3Key := strings.ReplaceAll(file.Name(), "-", "/")
		// 残留文件重传不覆盖已存在对象：原对象可能已被 consistency-checker 改写（is_fork）
		err = p.uploadFileToS3(&DataFile{
			S3key: s3Key,
			Data:  data,
		}, false)
		if err != nil {
			return err
		}
		// remove tmp file
		err = os.Remove(fullPath)
		if err != nil {
			return err
		}
	}
	go func() {
		for {
			select {
			case <-p.quitCh:
				return
			case dataFile := <-p.S3DataCh:
				go func() {
					err = p.UploadFileToS3(dataFile)
					if err != nil {
						log.Printf("failed to upload files to s3: %v", err)
						panic(err)
					}
					localfilePath := filepath.Join(p.S3TempDir, strings.ReplaceAll(dataFile.S3key, "/", "-"))
					err = os.Remove(localfilePath)
					if err != nil {
						log.Printf("failed to remove tmp file: %v", err)
					}
				}()
			}
		}
	}()
	return nil
}

func (p *PushProcessor) UploadFile(dataFile *DataFile) error {
	if p.S3TempDir != "" {
		localfilePath := filepath.Join(p.S3TempDir, strings.ReplaceAll(dataFile.S3key, "/", "-"))
		err := os.WriteFile(localfilePath, dataFile.Data, 0644)
		if err != nil {
			log.Printf("failed to write file: %v", err)
			return err
		}
		p.S3DataCh <- dataFile
		return nil
	} else {
		return p.UploadFileToS3(dataFile)
	}
}

func (p *PushProcessor) UploadFileToS3(file *DataFile) error {
	return p.uploadFileToS3(file, p.overwriteOnUpload(file))
}

// overwriteOnUpload 决定上传是否允许覆盖 S3 上已存在的对象。
// validation 对象会被 consistency-checker 原地改写 is_fork 标记，
// 覆盖上传会把标记冲回 false，因此一律不覆盖（内容确定性，已存在即跳过）。
func (p *PushProcessor) overwriteOnUpload(file *DataFile) bool {
	if file.Kind == "block_file_validation" {
		return false
	}
	return failover.IsLeaderNode()
}

func (p *PushProcessor) uploadFileToS3(file *DataFile, overWrite bool) error {
	start := time.Now()
	var err error
	defer func() {
		if err != nil {
			log.Printf("failed to upload file to s3: %v", err)
			return
		}
		if file.Kind == "block_file" {
			metrics.BlockFileUploadTimer.UpdateSince(start)
		}
		if file.Kind == "block_file_validation" {
			metrics.BlockFileValidationTimer.UpdateSince(start)
		}
		if file.Kind == "block_header" {
			metrics.BlockHeaderUploadTimer.UpdateSince(start)
		}
		if file.Kind == "state_diff" {
			metrics.StateDiffUploadTimer.UpdateSince(start)
		}
	}()
	times := 0
	for {
		err = util.UploadFileToS3(p.Uploader, p.Bucket, file.S3key, file.Data, overWrite)
		if err != nil {
			var apiErr smithy.APIError
			if (errors.As(err, &apiErr) && apiErr.ErrorCode() == "InternalServerException") || strings.Contains(err.Error(), "StatusCode: 500") ||
				strings.Contains(err.Error(), "InternalServerError") {
				log.Printf("HTTP 500 error detected, retrying in 1 second: %v", err)
			} else {
				// 任何错误都持续重试，避免上层 panic(err) 把节点搞挂。
				log.Printf("S3 upload error, retrying in 1 second (attempt %d): %v", times+1, err)
			}
			metrics.S3UploadRetryCounter.Inc(1)
			times++
			select {
			case <-p.quitCh:
				return nil
			case <-time.After(time.Second):
			}
			continue
		}
		break
	}
	return nil
}

func (p *PushProcessor) UploadFilesToS3(files []*DataFile) error {
	var wg sync.WaitGroup
	var errs []error
	var lock sync.Mutex
	for _, file := range files {
		wg.Add(1)
		go func(file *DataFile) {
			times := 0
			for {
				err := util.UploadFileToS3(p.Uploader, p.Bucket, file.S3key, file.Data, p.overwriteOnUpload(file))
				if err != nil {
					var apiErr smithy.APIError
					if (errors.As(err, &apiErr) && apiErr.ErrorCode() == "InternalServerException") || strings.Contains(err.Error(), "StatusCode: 500") ||
						strings.Contains(err.Error(), "InternalServerError") {
						log.Printf("HTTP 500 error detected, retrying in 1 second: %v", err)
						time.Sleep(time.Second)
						continue
					}
					if times > 3 {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						log.Printf("failed to upload file to s3: %s", err)
						wg.Done()
						return
					}
					time.Sleep(time.Second)
					times++
					continue
				}
				break
			}
			wg.Done()
		}(file)
	}
	wg.Wait()
	if len(errs) > 0 {
		return fmt.Errorf("failed to upload files to s3: %v", errs)
	}
	return nil
}

func (p *PushProcessor) LastPushedBlock() *types.BlockContext {
	p.noticeMu.RLock()
	defer p.noticeMu.RUnlock()
	return p.lastPushedBlockLocked()
}

func (p *PushProcessor) PushBlockChangeNotification(blockNotice *types.BlockChangeNotification) error {
	if blockNotice == nil || len(blockNotice.NewBlocks) == 0 {
		return fmt.Errorf("block change notification has no new blocks")
	}
	if failover.GlobalManager == nil {
		return fmt.Errorf("failover manager is not initialized")
	}
	failover.GlobalManager.Mutex.Lock()
	defer failover.GlobalManager.Mutex.Unlock()
	p.noticeMu.Lock()
	defer p.noticeMu.Unlock()

	if !failover.GlobalManager.IsLeaderLocked() {
		log.Printf("node is not the active leader, skip push block change notification\n")
		return nil
	}

	if len(blockNotice.NewBlocks) > 1 {
		// 1. 首先检查 newBlocks 是否满足我们想要的严格顺序和父子关系
		valid := true
		for i := 0; i < len(blockNotice.NewBlocks)-1; i++ {
			current := blockNotice.NewBlocks[i]
			next := blockNotice.NewBlocks[i+1]

			// 2. 检查区块高度是否递增
			if current.BlockNumber+1 != next.BlockNumber {
				valid = false
				log.Printf("block number not in strict order: %d, %d", current.BlockNumber, next.BlockNumber)
				break
			}

			// 3. 检查当前区块的哈希是否匹配下一个区块的父哈希
			if current.Hash != next.ParentHash {
				valid = false
				log.Printf("parent hash not match: %s, %s", current.Hash, next.ParentHash)
				break
			}
		}
		if !valid {
			return fmt.Errorf("new blocks not in strict order or parent-child relationship")
		}
	}

	lastPushedBlock := p.lastPushedBlockLocked()
	if lastPushedBlock == nil && blockNotice.NewBlocks[0].BlockNumber != 0 {
		return fmt.Errorf("last pushed block is empty but new block number is not 0")
	}

	if lastPushedBlock != nil &&
		(lastPushedBlock.BlockNumber >= blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber) {
		return nil
	}

	if lastPushedBlock != nil {
		if blockNotice.ChangeType == 1 {
			if lastPushedBlock.Hash != blockNotice.NewBlocks[0].ParentHash {
				return fmt.Errorf("last pushed block hash is not equal to new block parent hash")
			}
		}
		if blockNotice.ChangeType == 2 {
			if len(blockNotice.DropBlocks) == 0 || lastPushedBlock.Hash != blockNotice.DropBlocks[len(blockNotice.DropBlocks)-1].Hash {
				return fmt.Errorf("last pushed block hash is not equal to drop block hash")
			}
		}
	}

	start := time.Now()
	defer func() {
		metrics.BlockPushTimer.UpdateSince(start)

	}()
	// 将区块变更通知写入Kafka
	err := util.WriteBlockNotice(p.KafkaWriter, blockNotice)
	if err != nil {
		return fmt.Errorf("写入区块变更通知到Kafka失败: %v", err)
	}

	// 更新最新的区块通知
	p.LastBlockNotice = blockNotice
	metrics.LatestBlockNumber.Update(int64(blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber))
	metrics.LatestBlockTime.Update(int64(blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].Timestamp))
	return nil
}

func (p *PushProcessor) lastPushedBlockLocked() *types.BlockContext {
	if p.LastBlockNotice == nil || len(p.LastBlockNotice.NewBlocks) == 0 {
		return nil
	}
	last := p.LastBlockNotice.NewBlocks[len(p.LastBlockNotice.NewBlocks)-1]
	return &last
}

// reloadEmptyLastBlock 在主节点的 LastBlockNotice 为空时限频重新从 Kafka 读取最后一条通知。
// 启动时 topic 为空（比如种子消息晚于节点启动写入）会导致 LastBlockNotice 一直为 nil，
// 而它只在启动或晋升时读取一次。读 Kafka 有网络调用，因此在不持有 failover Mutex 的情况下执行。
func (p *PushProcessor) reloadEmptyLastBlock() {
	if !failover.IsLeaderNode() || p.LastPushedBlock() != nil {
		return
	}
	now := time.Now().Unix()
	last := p.lastBlockReloadAt.Load()
	if now-last < lastBlockReloadInterval || !p.lastBlockReloadAt.CompareAndSwap(last, now) {
		return
	}

	kafkaReader := util.NewKafkaReader(p.Brokers, p.Topic, "")
	defer kafkaReader.Close()
	lastBlockNotice, err := util.GetLastBlockNotice(kafkaReader)
	if err != nil {
		log.Printf("failed to reload last block notice: %v\n", err)
		return
	}
	log.Printf("reload last block notice: %+v\n", lastBlockNotice)

	p.noticeMu.Lock()
	if p.LastBlockNotice == nil {
		p.LastBlockNotice = lastBlockNotice
	}
	p.noticeMu.Unlock()
}

// NotifyBlockCommit is called by geth when a block is committed to the canonical chain.
// It checks leader status first, then computes reorg if needed, and pushes to kafka.
// Backup nodes return immediately without any computation.
//
// firstSeenAt is accepted for API compatibility with upstream pipeline but is not
// forwarded: this vendored copy does not encode block first-seen headers.
func (p *PushProcessor) NotifyBlockCommit(block interface {
	NumberU64() uint64
	Hash() common.Hash
	ParentHash() common.Hash
	Time() uint64
}, bc BlockChainReader, firstSeenAt map[common.Hash]int64) error {
	if failover.GlobalManager == nil {
		return fmt.Errorf("failover manager is not initialized")
	}

	p.reloadEmptyLastBlock()

	failover.GlobalManager.Mutex.Lock()
	defer failover.GlobalManager.Mutex.Unlock()
	p.noticeMu.Lock()
	defer p.noticeMu.Unlock()

	// 1. Check leader status first - backup nodes return immediately without any computation
	if !failover.GlobalManager.IsLeaderLocked() {
		log.Printf("backup node: skip block commit notification\n")
		return nil
	}

	// 2. Only leader nodes proceed - check if we need to push anything
	lastPushedBlock := p.lastPushedBlockLocked()

	// If last pushed block is newer than current block, skip (e.g., after unwind)
	if lastPushedBlock != nil && lastPushedBlock.BlockNumber > block.NumberU64() {
		log.Printf("last pushed block %d is newer than current block %d, skip\n", lastPushedBlock.BlockNumber, block.NumberU64())
		return nil
	}

	// If no last pushed block and this is not genesis, error
	if lastPushedBlock == nil && block.NumberU64() != 0 {
		return fmt.Errorf("last pushed block is empty but new block number is not 0")
	}

	// 3. Compute common ancestor and determine what to push
	currentBlock := types.BlockContext{
		BlockNumber: block.NumberU64(),
		Hash:        block.Hash(),
		ParentHash:  block.ParentHash(),
		Timestamp:   block.Time(),
	}

	var blockChange *types.BlockChangeNotification

	if lastPushedBlock != nil && lastPushedBlock.BlockNumber <= block.NumberU64() {
		_, dropBlocks, newBlocks, err := p.getCommonAncestor(bc, *lastPushedBlock, currentBlock)
		if err != nil {
			// header 缺失时不退出进程，下一个块提交时会重新计算。
			return fmt.Errorf("compute common ancestor: %w", err)
		}

		if len(dropBlocks) > 0 {
			// Fork/reorg case
			blockChange = &types.BlockChangeNotification{
				ChangeType: 2,
				NewBlocks:  newBlocks,
				DropBlocks: dropBlocks,
			}
		} else if len(newBlocks) > 0 {
			// Normal case: new blocks on canonical chain
			blockChange = &types.BlockChangeNotification{
				ChangeType: 1,
				NewBlocks:  newBlocks,
			}
		}
	} else if lastPushedBlock == nil && block.NumberU64() == 0 {
		// Genesis block
		blockChange = &types.BlockChangeNotification{
			ChangeType: 1,
			NewBlocks:  []types.BlockContext{currentBlock},
		}
	}

	// 4. Push to kafka if there's something to push
	if blockChange != nil {
		if err := p.pushBlockChangeNotificationLocked(blockChange, firstSeenAt); err != nil {
			return fmt.Errorf("failed to push block change notification: %w", err)
		}
		log.Printf("pushed block change notification: changeType=%d, dropBlocks=%d, newBlocks=%d\n",
			blockChange.ChangeType, len(blockChange.DropBlocks), len(blockChange.NewBlocks))
	}

	return nil
}

// pushBlockChangeNotificationLocked is the internal implementation that assumes locks are held.
// firstSeenAt is not forwarded: this vendored util.WriteBlockNotice has no first-seen headers.
func (p *PushProcessor) pushBlockChangeNotificationLocked(blockNotice *types.BlockChangeNotification, firstSeenAt map[common.Hash]int64) error {
	if blockNotice == nil || len(blockNotice.NewBlocks) == 0 {
		return fmt.Errorf("block change notification has no new blocks")
	}

	// Validate new blocks are in strict order and parent-child relationship
	if len(blockNotice.NewBlocks) > 1 {
		for i := 0; i < len(blockNotice.NewBlocks)-1; i++ {
			current := blockNotice.NewBlocks[i]
			next := blockNotice.NewBlocks[i+1]

			if current.BlockNumber+1 != next.BlockNumber {
				return fmt.Errorf("block number not in strict order: %d, %d", current.BlockNumber, next.BlockNumber)
			}

			if current.Hash != next.ParentHash {
				return fmt.Errorf("parent hash not match: %s, %s", current.Hash, next.ParentHash)
			}
		}
	}

	// Validate against last pushed block
	lastPushedBlock := p.lastPushedBlockLocked()
	if lastPushedBlock != nil {
		// Skip if we're trying to push old blocks
		if lastPushedBlock.BlockNumber >= blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber {
			return nil
		}

		// Validate change type consistency
		if blockNotice.ChangeType == 1 {
			if lastPushedBlock.Hash != blockNotice.NewBlocks[0].ParentHash {
				return fmt.Errorf("last pushed block hash is not equal to new block parent hash")
			}
		}
		if blockNotice.ChangeType == 2 {
			if len(blockNotice.DropBlocks) == 0 || lastPushedBlock.Hash != blockNotice.DropBlocks[len(blockNotice.DropBlocks)-1].Hash {
				return fmt.Errorf("last pushed block hash is not equal to drop block hash")
			}
		}
	}

	start := time.Now()
	defer func() {
		metrics.BlockPushTimer.UpdateSince(start)
	}()

	// Write to Kafka
	err := util.WriteBlockNotice(p.KafkaWriter, blockNotice)
	if err != nil {
		return fmt.Errorf("failed to write block notice to kafka: %w", err)
	}

	// Update last pushed block
	p.LastBlockNotice = blockNotice
	metrics.LatestBlockNumber.Update(int64(blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber))
	metrics.LatestBlockTime.Update(int64(blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].Timestamp))
	return nil
}

// getCommonAncestor returns the common ancestor of two blocks and their paths from the ancestor
func (p *PushProcessor) getCommonAncestor(bc BlockChainReader, blocka types.BlockContext, blockb types.BlockContext) (types.BlockContext, []types.BlockContext, []types.BlockContext, error) {
	var (
		chainA, chainB []types.BlockContext
	)

	// Fast path: blockb is direct child of blocka
	if blockb.ParentHash == blocka.Hash {
		return blocka, chainA, []types.BlockContext{blockb}, nil
	}

	// Bring blockb down to same height as blocka
	for blockb.BlockNumber > blocka.BlockNumber {
		chainB = append(chainB, blockb)
		headerb := bc.GetHeaderByHash2(blockb.ParentHash)
		if headerb == nil {
			return types.BlockContext{}, nil, nil, fmt.Errorf("header not found: %s", blockb.ParentHash)
		}
		blockb = types.BlockContext{
			BlockNumber: headerb.Number.Uint64(),
			Hash:        headerb.Hash(),
			ParentHash:  headerb.ParentHash,
			Timestamp:   headerb.Time,
		}
	}

	// 祖先不可能低于创世块；超过这个深度一定是数据异常，
	// 继续走会在创世块的空 parent 上死循环。
	const maxWalk = 100000

	// Walk both chains back until we find common ancestor
	for blocka.Hash != blockb.Hash {
		if len(chainA) > maxWalk {
			return types.BlockContext{}, nil, nil, fmt.Errorf("common ancestor deeper than %d blocks", maxWalk)
		}
		chainA = append(chainA, blocka)
		headera := bc.GetHeaderByHash2(blocka.ParentHash)
		if headera == nil {
			return types.BlockContext{}, nil, nil, fmt.Errorf("header not found: %s", blocka.ParentHash)
		}
		blocka = types.BlockContext{
			BlockNumber: headera.Number.Uint64(),
			Hash:        headera.Hash(),
			ParentHash:  headera.ParentHash,
			Timestamp:   headera.Time,
		}

		chainB = append(chainB, blockb)
		headerb := bc.GetHeaderByHash2(blockb.ParentHash)
		if headerb == nil {
			return types.BlockContext{}, nil, nil, fmt.Errorf("header not found: %s", blockb.ParentHash)
		}
		blockb = types.BlockContext{
			BlockNumber: headerb.Number.Uint64(),
			Hash:        headerb.Hash(),
			ParentHash:  headerb.ParentHash,
			Timestamp:   headerb.Time,
		}
	}

	// Now blocka == blockb == ancestor
	// Reverse chains so they're in ascending order
	for i, j := 0, len(chainA)-1; i < j; i, j = i+1, j-1 {
		chainA[i], chainA[j] = chainA[j], chainA[i]
	}
	for i, j := 0, len(chainB)-1; i < j; i, j = i+1, j-1 {
		chainB[i], chainB[j] = chainB[j], chainB[i]
	}

	return blocka, chainA, chainB, nil
}

func (p *PushProcessor) Close() {
	p.KafkaWriter.Close()
	close(p.quitCh)
}
