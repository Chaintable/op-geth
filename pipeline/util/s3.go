package util

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/ethereum/go-ethereum/log"
)

func NewS3Client(region string) (*s3.Client, error) {
	log.Info("Init S3 client: load config", "region", region)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Error("Init S3 client: load config timeout", "region", region)
		}
		return nil, err
	}
	cfg.Region = region
	log.Info("Init S3 client: load config done", "region", region)
	client := s3.NewFromConfig(cfg)
	return client, nil
}

func UploadFileToS3(uploader *s3.Client, bucket string, key string, data []byte, overWrite bool) error {
	input := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}
	if !overWrite {
		input.IfNoneMatch = aws.String("*")
	}

	_, err := uploader.PutObject(context.TODO(), input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
			return nil
		}
	}
	return err
}
func DownloadFileFromS3(downloader *s3.Client, bucket string, key string) ([]byte, error) {
	output, err := downloader.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(output.Body)
	return buf.Bytes(), nil
}

func DownloadFileFromS3Json(downloader *s3.Client, bucket string, key string, target any) error {
	b, err := DownloadFileFromS3(downloader, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	return DecodeFromGzipJson(b, target)
}

func DownloadFileFromS3Rlp(downloader *s3.Client, bucket string, key string, target any) error {
	b, err := DownloadFileFromS3(downloader, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	return DecodeFromRlp(b, target)
}
