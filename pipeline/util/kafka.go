package util

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/segmentio/kafka-go"
)

func NewKafkaReader(brokers []string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
}

// 获取最后一个BlockChangeNotification
func GetLastBlockNotice(reader *kafka.Reader) (*types.BlockChangeNotification, error) {
	reader.SetOffset(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	lag, err := reader.ReadLag(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("kafka ReadLag timeout after %s\n", time.Since(start))
			return nil, nil
		}
		return nil, err
	}
	fmt.Printf("kafka ReadLag ok: lag=%d elapsed=%s\n", lag, time.Since(start))
	if lag == 0 {
		return nil, nil
	}

	err = reader.SetOffset(lag - 1)
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start = time.Now()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("kafka ReadMessage timeout after %s\n", time.Since(start))
			return nil, nil
		}
		return nil, err
	}
	fmt.Printf("kafka ReadMessage ok: key=%s elapsed=%s\n", string(msg.Key), time.Since(start))

	if !bytes.Equal(msg.Key, []byte("NewBlock")) {
		return nil, fmt.Errorf("last message is not NewBlock")
	}

	blockNotice := &types.BlockChangeNotification{}
	err = DecodeFromGzipJson(msg.Value, blockNotice)
	if err != nil {
		return nil, err
	}

	return blockNotice, nil
}

func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		// 默认100个，或者等待1s才发生
		BatchBytes: 1024 * 1024 * 10,
		BatchSize:  1,
	}
}

func WriteBlockNotice(writer *kafka.Writer, blockNotice *types.BlockChangeNotification) error {
	value, err := EncodeToJsonGzip(blockNotice)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("NewBlock"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func WriteOuterBlockNotice(writer *kafka.Writer, outerBlockNotice *types.OuterBlockChangeNotification) error {
	value, err := EncodeToJsonGzip(outerBlockNotice)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("NewBlock"),
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}
