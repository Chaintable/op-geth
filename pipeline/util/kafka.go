package util

import (
	"bytes"
	"context"
	"fmt"

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
	lag, err := reader.ReadLag(context.Background())
	if err != nil {
		return nil, err
	}
	if lag == 0 {
		return nil, nil
	}

	err = reader.SetOffset(lag - 1)
	if err != nil {
		return nil, err
	}

	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return nil, err
	}

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
		BatchSize: 1,
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
