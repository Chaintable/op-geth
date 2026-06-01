package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/Chaintable/pipeline/processor"
	ptypes "github.com/Chaintable/pipeline/types"
	putil "github.com/Chaintable/pipeline/util"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/segmentio/kafka-go"
)

const (
	snapshotDumpS3SinglePutObjectLimit = int64(5_000_000_000)
	snapshotDumpS3MultipartPartSize    = int64(64 * 1024 * 1024)
	snapshotDumpS3MultipartMaxParts    = int64(10000)
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

func (p *snapshotDumpPublisher) uploadNodeXLocalFile(key, kind, path string) error {
	return p.uploadLocalFile(p.nodeXBucket, key, kind, path)
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

func (p *snapshotDumpPublisher) uploadLocalFile(bucket, key, kind, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open local dump file %s: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat local dump file %s: %w", path, err)
	}
	if info.Size() > snapshotDumpS3SinglePutObjectLimit {
		return p.uploadMultipartLocalFile(file, bucket, key, kind, info.Size())
	}
	if _, err := p.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          file,
		ContentLength: ptrInt64(info.Size()),
	}); err != nil {
		return fmt.Errorf("failed to upload %s to s3 bucket %s: %w", key, bucket, err)
	}
	log.Info("Uploaded snapshot dump file to s3", "bucket", bucket, "key", key, "kind", kind, "size", common.StorageSize(info.Size()))
	return nil
}

func (p *snapshotDumpPublisher) uploadMultipartLocalFile(file *os.File, bucket, key, kind string, size int64) error {
	ctx := context.TODO()
	partSize := snapshotDumpS3MultipartPartSize
	if minPartSize := (size + snapshotDumpS3MultipartMaxParts - 1) / snapshotDumpS3MultipartMaxParts; minPartSize > partSize {
		const mib = int64(1024 * 1024)
		partSize = ((minPartSize + mib - 1) / mib) * mib
	}
	upload, err := p.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to start multipart upload %s to s3 bucket %s: %w", key, bucket, err)
	}
	abort := func(cause error) error {
		if _, err := p.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   &bucket,
			Key:      &key,
			UploadId: upload.UploadId,
		}); err != nil {
			return fmt.Errorf("%w; failed to abort multipart upload: %v", cause, err)
		}
		return cause
	}
	buffer := make([]byte, int(partSize))
	parts := make([]s3types.CompletedPart, 0, int((size+partSize-1)/partSize))
	for partNumber := int32(1); ; partNumber++ {
		n, readErr := io.ReadFull(file, buffer)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return abort(fmt.Errorf("failed to read local dump file for multipart upload: %w", readErr))
		}
		uploaded, err := p.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        &bucket,
			Key:           &key,
			UploadId:      upload.UploadId,
			PartNumber:    ptrInt32(partNumber),
			Body:          bytes.NewReader(buffer[:n]),
			ContentLength: ptrInt64(int64(n)),
		})
		if err != nil {
			return abort(fmt.Errorf("failed to upload multipart part %d for %s: %w", partNumber, key, err))
		}
		parts = append(parts, s3types.CompletedPart{
			ETag:       uploaded.ETag,
			PartNumber: ptrInt32(partNumber),
		})
		log.Info("Uploaded snapshot dump multipart part", "bucket", bucket, "key", key, "kind", kind, "part", partNumber, "size", common.StorageSize(n))
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if _, err := p.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: upload.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: parts,
		},
	}); err != nil {
		return abort(fmt.Errorf("failed to complete multipart upload %s to s3 bucket %s: %w", key, bucket, err))
	}
	log.Info("Uploaded snapshot dump file to s3", "bucket", bucket, "key", key, "kind", kind, "size", common.StorageSize(size), "parts", len(parts))
	return nil
}

func ptrInt32(v int32) *int32 {
	return &v
}

func ptrInt64(v int64) *int64 {
	return &v
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
