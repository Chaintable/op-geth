package main

import (
	"fmt"

	"github.com/Chaintable/pipeline/processor"
	ptypes "github.com/Chaintable/pipeline/types"
	putil "github.com/Chaintable/pipeline/util"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/segmentio/kafka-go"
)

type snapshotDumpPublisher struct {
	s3Client         *s3.Client
	kafkaWriter      *kafka.Writer
	nodeXBucket      string
	chainTableBucket string
}

func newSnapshotDumpPublisher(region, nodeXBucket, chainTableBucket string, brokers []string, topic string) (*snapshotDumpPublisher, error) {
	s3Client, err := putil.NewS3Client(region)
	if err != nil {
		return nil, err
	}
	return &snapshotDumpPublisher{
		s3Client:         s3Client,
		kafkaWriter:      putil.NewKafkaWriter(brokers, topic),
		nodeXBucket:      nodeXBucket,
		chainTableBucket: chainTableBucket,
	}, nil
}

func (p *snapshotDumpPublisher) uploadNodeX(file *processor.DataFile) error {
	return p.uploadFile(p.nodeXBucket, file)
}

func (p *snapshotDumpPublisher) uploadChainTable(file *processor.DataFile) error {
	return p.uploadFile(p.chainTableBucket, file)
}

func (p *snapshotDumpPublisher) uploadFile(bucket string, file *processor.DataFile) error {
	if err := putil.UploadFileToS3(p.s3Client, bucket, file.S3key, file.Data, true); err != nil {
		return fmt.Errorf("failed to upload %s to s3 bucket %s: %w", file.S3key, bucket, err)
	}
	log.Info("Uploaded snapshot dump file to s3", "bucket", bucket, "key", file.S3key, "kind", file.Kind, "size", common.StorageSize(len(file.Data)))
	return nil
}

func (p *snapshotDumpPublisher) pushBlockChangeNotification(blockChanges *ptypes.BlockChangeNotification) error {
	if err := putil.WriteBlockNotice(p.kafkaWriter, blockChanges); err != nil {
		return fmt.Errorf("failed to write block change notification to kafka: %w", err)
	}
	return nil
}

func (p *snapshotDumpPublisher) close() error {
	if p.kafkaWriter == nil {
		return nil
	}
	return p.kafkaWriter.Close()
}
