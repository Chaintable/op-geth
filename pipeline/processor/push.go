package processor

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/pipeline/metrics"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	IsBackup        bool
}

func NewPushProcessor(region string, bucket string, brokers []string, topic string, s3TempDir string, isBackup bool) (*PushProcessor, error) {
	kafkaReader := util.NewKafkaReader(brokers, topic, "")
	kafkaWriter := util.NewKafkaWriter(brokers, topic)
	s3Uploader, err := util.NewS3Client(region)
	if err != nil {
		return nil, err
	}

	var lastBlockNotice *types.BlockChangeNotification

	lastBlockNotice, err = util.GetLastBlockNotice(kafkaReader)
	if err != nil {
		return nil, err
	}
	defer kafkaReader.Close()

	log.Printf("last block notice: %+v\n", lastBlockNotice)

	if s3TempDir != "" {
		s3TempDir = filepath.Join(s3TempDir, bucket)
	}

	pusher := &PushProcessor{
		Bucket:          bucket,
		Uploader:        s3Uploader,
		KafkaWriter:     kafkaWriter,
		LastBlockNotice: lastBlockNotice,
		S3TempDir:       s3TempDir,
		quitCh:          make(chan struct{}),
		S3DataCh:        make(chan *DataFile, 100),
		IsBackup:        isBackup,
	}

	if s3TempDir != "" {
		err = pusher.uploadWork()
		if err != nil {
			return nil, err
		}
	}
	return pusher, nil
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
		err = p.UploadFileToS3(&DataFile{
			S3key: s3Key,
			Data:  data,
		})
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
		err = util.UploadFileToS3(p.Uploader, p.Bucket, file.S3key, file.Data, !p.IsBackup)
		if err != nil {
			if times > 3 {
				return err
			}
			time.Sleep(time.Second)
			times++
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
				err := util.UploadFileToS3(p.Uploader, p.Bucket, file.S3key, file.Data, !p.IsBackup)
				if err != nil {
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
	if p.LastBlockNotice == nil {
		return nil
	}
	return &p.LastBlockNotice.NewBlocks[len(p.LastBlockNotice.NewBlocks)-1]
}

func (p *PushProcessor) PushBlockChangeNotification(blockNotice *types.BlockChangeNotification) error {
	if p.IsBackup {
		// 如果是备份模式，直接返回
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

	if p.LastPushedBlock() == nil && blockNotice.NewBlocks[0].BlockNumber != 0 {
		return fmt.Errorf("last pushed block is empty but new block number is not 0")
	}

	if p.LastPushedBlock() != nil &&
		(p.LastPushedBlock().BlockNumber >= blockNotice.NewBlocks[len(blockNotice.NewBlocks)-1].BlockNumber) {
		return nil
	}

	if p.LastPushedBlock() != nil {
		if blockNotice.ChangeType == 1 {
			if p.LastPushedBlock().Hash != blockNotice.NewBlocks[0].ParentHash {
				return fmt.Errorf("last pushed block hash is not equal to new block parent hash")
			}
		}
		if blockNotice.ChangeType == 2 {
			if p.LastPushedBlock().Hash != blockNotice.DropBlocks[len(blockNotice.DropBlocks)-1].Hash {
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

func (p *PushProcessor) Close() {
	p.KafkaWriter.Close()
	close(p.quitCh)
}
