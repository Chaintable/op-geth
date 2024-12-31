package misc

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func TestEnsureCreate2Deployer(t *testing.T) {
	canyonTime := uint64(1000)
	tests := []struct {
		name       string
		override   func(cfg *params.ChainConfig)
		timestamp  uint64
		codeExists bool
		applied    bool
	}{
		{
			name:      "at hardfork",
			timestamp: canyonTime,
			applied:   true,
		},
		{
			name: "another chain ID",
			override: func(cfg *params.ChainConfig) {
				cfg.ChainID = big.NewInt(params.OPMainnetChainID)
			},
			timestamp: canyonTime,
			applied:   true,
		},
		{
			name:       "code already exists",
			timestamp:  canyonTime,
			codeExists: true,
			applied:    true,
		},
		{
			name:      "pre canyon",
			timestamp: canyonTime - 1,
			applied:   false,
		},
		{
			name:      "post hardfork",
			timestamp: canyonTime + 1,
			applied:   false,
		},
		{
			name: "canyon not configured",
			override: func(cfg *params.ChainConfig) {
				cfg.CanyonTime = nil
			},
			timestamp: canyonTime,
			applied:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := params.ChainConfig{
				ChainID:    big.NewInt(params.BaseMainnetChainID),
				Optimism:   &params.OptimismConfig{},
				CanyonTime: &canyonTime,
			}
			if tt.override != nil {
				tt.override(&cfg)
			}
			state := &stateDb{
				codeMap: map[common.Address][]byte{},
			}
			EnsureCreate2Deployer(&cfg, tt.timestamp, state)
			assert.Equal(t, tt.applied, state.GetCodeSize(create2DeployerAddress) > 0)
		})
	}
}

type stateDb struct {
	vm.StateDB
	codeMap map[common.Address][]byte
	storage map[common.Address]map[common.Hash]common.Hash
}

func (s *stateDb) GetCode(addr common.Address) []byte {
	s.initCodeMap()
	return s.codeMap[addr]
}

func (s *stateDb) GetCodeSize(addr common.Address) int {
	s.initCodeMap()
	return len(s.codeMap[addr])
}

func (s *stateDb) SetCode(addr common.Address, code []byte) []byte {
	s.initCodeMap()
	s.codeMap[addr] = code
	return nil
}

func (s *stateDb) initCodeMap() {
	if s.codeMap == nil {
		s.codeMap = make(map[common.Address][]byte)
	}
}

func (s *stateDb) GetState(addr common.Address, key common.Hash) common.Hash {
	if s.storage == nil {
		return common.Hash{}
	}
	if _, ok := s.storage[addr]; !ok {
		return common.Hash{}
	}

	return s.storage[addr][key]
}

func (s *stateDb) SetState(addr common.Address, key common.Hash, value common.Hash) common.Hash {
	if s.storage == nil {
		s.storage = make(map[common.Address]map[common.Hash]common.Hash)
	}
	if _, ok := s.storage[addr]; !ok {
		s.storage[addr] = map[common.Hash]common.Hash{key: value}
	} else {
		s.storage[addr][key] = value
	}

	return common.Hash{}
}
