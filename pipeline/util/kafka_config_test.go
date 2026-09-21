package util

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaWriterConfiguration(t *testing.T) {
	writer := NewKafkaWriter([]string{"broker:9092"}, "existing-topic")
	defer writer.Close()

	if writer.RequiredAcks != kafka.RequireAll {
		t.Errorf("RequiredAcks = %v, want %v", writer.RequiredAcks, kafka.RequireAll)
	}
	if writer.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation = true, want false")
	}
}
