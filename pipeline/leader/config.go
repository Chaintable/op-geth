package leader

import (
	"context"
	"time"
)

// Config is the configuration for etcd-based failover mode
type Config struct {
	Endpoints   []string
	Key         string
	NodeID      string
	GracePeriod time.Duration
}

// LeaderCallbacks defines callbacks for leader state changes
type LeaderCallbacks struct {
	OnBecomeLeader func(ctx context.Context) error
	OnLoseLeader   func(ctx context.Context) error
}