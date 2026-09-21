package metrics

import (
	"testing"

	gethmetrics "github.com/ethereum/go-ethereum/metrics"
)

func TestKafkaWriteTimerUsesTopicInMetricName(t *testing.T) {
	const firstTopic = "pipeline-metrics-test-one"
	const secondTopic = "pipeline-metrics-test-two"

	// 未启用 metrics 时所有 timer 都是同一个 NilResettingTimer
	enabled := gethmetrics.Enabled
	gethmetrics.Enabled = true
	defer func() { gethmetrics.Enabled = enabled }()

	firstTimer := KafkaWriteTimer(firstTopic)
	if firstTimer != KafkaWriteTimer(firstTopic) {
		t.Fatal("expected the timer for a topic to be reused")
	}
	if firstTimer == KafkaWriteTimer(secondTopic) {
		t.Fatal("expected different topics to use different timers")
	}
	if gethmetrics.DefaultRegistry.Get("pipeline/kafka_write/"+firstTopic) != firstTimer {
		t.Fatal("topic timer was not registered with the topic in its metric name")
	}
}
