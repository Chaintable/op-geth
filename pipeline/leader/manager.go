package leader

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// GlobalManager is a global reference to the leader manager
var GlobalManager *Manager

type Manager struct {
	LeaderFailover *LeaderFailover // etcd-based failover mode
	ManualMode     bool            // Fixed mode (no etcd)
	IsManualBackup bool
	config         *ManagerConfig
}

type ManagerConfig struct {
	EtcdEndpoints  []string
	ElectionKey    string
	NodeID         string
	IsBackup       *bool // nil = use etcd failover, non-nil = fixed mode
	OnBecomeLeader func() error
	OnLoseLeader   func() error
	GracePeriod    time.Duration
}

func NewManager(cfg *ManagerConfig) (*Manager, error) {
	m := &Manager{
		config: cfg,
	}

	if cfg.IsBackup != nil {
		// Fixed mode - set fixed backup state (no etcd needed)
		m.ManualMode = true
		m.IsManualBackup = *cfg.IsBackup
		log.Printf("NodeID: %s Created in fixed mode, isBackup=%v", cfg.NodeID, *cfg.IsBackup)
	} else {
		// Use etcd-based failover
		electionCfg := Config{
			Endpoints:   cfg.EtcdEndpoints,
			Key:         cfg.ElectionKey,
			NodeID:      cfg.NodeID,
			GracePeriod: cfg.GracePeriod,
		}

		callbacks := LeaderCallbacks{
			OnBecomeLeader: func(ctx context.Context) error {
				log.Printf("NodeID: %s Becoming leader", cfg.NodeID)
				if cfg.OnBecomeLeader != nil {
					return cfg.OnBecomeLeader()
				}
				return nil
			},
			OnLoseLeader: func(ctx context.Context) error {
				log.Printf("NodeID: %s Losing leader", cfg.NodeID)
				if cfg.OnLoseLeader != nil {
					return cfg.OnLoseLeader()
				}
				return nil
			},
		}

		leaderFailover, err := NewLeaderFailover(electionCfg)
		if err != nil {
			return nil, err
		}
		leaderFailover.SetCallbacks(callbacks)
		m.LeaderFailover = leaderFailover
		log.Printf("NodeID: %s Created with failover mode", cfg.NodeID)
	}

	return m, nil
}

func (m *Manager) Start() error {
	if m.ManualMode && !m.IsManualBackup {
		return m.config.OnBecomeLeader()
	}
	if m.LeaderFailover != nil {
		return m.LeaderFailover.Start()
	}
	return nil
}

func (m *Manager) IsLeader() bool {
	if m.ManualMode {
		return !m.IsManualBackup
	}
	if m.LeaderFailover != nil {
		return m.LeaderFailover.IsLeader()
	}
	return false
}

// IsLeaderLocked is like IsLeader but must be called while LeaderMutex is
// already held (read or write) by the caller. Avoids a double-lock in hot paths.
func (m *Manager) IsLeaderLocked() bool {
	if m.ManualMode {
		return !m.IsManualBackup
	}
	if m.LeaderFailover != nil {
		return m.LeaderFailover.IsLeaderLocked()
	}
	return false
}

func (m *Manager) IsBackup() bool {
	if m.ManualMode {
		return m.IsManualBackup
	}
	if m.LeaderFailover != nil {
		return m.LeaderFailover.IsBackup()
	}
	return true // Default to backup
}

func (m *Manager) Lock() {
	if m.LeaderFailover != nil {
		m.LeaderFailover.LeaderMutex.Lock()
	}
}

func (m *Manager) Unlock() {
	if m.LeaderFailover != nil {
		m.LeaderFailover.LeaderMutex.Unlock()
	}
}

func (m *Manager) RLock() {
	if m.LeaderFailover != nil {
		m.LeaderFailover.LeaderMutex.RLock()
	}
}

func (m *Manager) RUnlock() {
	if m.LeaderFailover != nil {
		m.LeaderFailover.LeaderMutex.RUnlock()
	}
}

func (m *Manager) Stop() error {
	if m.LeaderFailover != nil {
		return m.LeaderFailover.Stop()
	}
	return nil
}

func (m *Manager) Close() error {
	if m.LeaderFailover != nil {
		return m.LeaderFailover.Close()
	}
	return nil
}

// GetEtcdClient returns the etcd client from the leader failover instance
func (m *Manager) GetEtcdClient() *clientv3.Client {
	if m.LeaderFailover != nil {
		return m.LeaderFailover.client
	}
	return nil
}