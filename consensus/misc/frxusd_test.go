package misc

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/assert"
)

func TestEnsureFrxUSDMainnet(t *testing.T) {
	graniteTime := uint64(1000)
	var tests = []struct {
		name       string
		override   func(cfg *params.ChainConfig)
		timestamp  uint64
		codeExists bool
		applied    bool
	}{
		{
			name:      "at hardfork",
			timestamp: graniteTime,
			applied:   true,
		},
		{
			name: "another chain ID",
			override: func(cfg *params.ChainConfig) {
				cfg.ChainID = big.NewInt(params.OPMainnetChainID)
			},
			timestamp: graniteTime,
			applied:   true,
		},
		{
			name:       "code already exists",
			timestamp:  graniteTime,
			codeExists: true,
			applied:    true,
		},
		{
			name:      "pre hardfork",
			timestamp: graniteTime - 1,
			applied:   false,
		},
		{
			name:      "post hardfork",
			timestamp: graniteTime + 1,
			applied:   false,
		},
		{
			name: "canyon not configured",
			override: func(cfg *params.ChainConfig) {
				cfg.GraniteTime = nil
			},
			timestamp: graniteTime,
			applied:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := params.ChainConfig{
				ChainID:     big.NewInt(252),
				Optimism:    &params.OptimismConfig{},
				GraniteTime: &graniteTime,
			}
			if tt.override != nil {
				tt.override(&cfg)
			}
			state := &stateDb{
				codeMap: map[common.Address][]byte{
					common.HexToAddress("0xfc0000000000000000000000000000000000000a"): {0},
					common.HexToAddress("0xfc00000000000000000000000000000000000001"): make([]byte, 10000),
					common.HexToAddress("0xfc00000000000000000000000000000000000008"): make([]byte, 10000),
				},
			}
			EnsureFrxUSD(&cfg, tt.timestamp, state)
			assert.Equal(t, tt.applied, state.GetCodeSize(common.HexToAddress("0xfcc0d30000000000000000000000000000000001")) > 0)
			assert.Equal(t, tt.applied, state.GetCodeSize(common.HexToAddress("0xfcc0d30000000000000000000000000000000008")) > 0)
			if tt.applied {
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000001")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000008")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
			} else {
				assert.NotEqual(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000001")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
				assert.NotEqual(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000008")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
			}
		})
	}
}

func TestEnsureFrxUSDTestnet(t *testing.T) {
	graniteTime := uint64(1000)
	var tests = []struct {
		name       string
		override   func(cfg *params.ChainConfig)
		timestamp  uint64
		codeExists bool
		applied    bool
	}{
		{
			name:      "at hardfork",
			timestamp: graniteTime,
			applied:   true,
		},
		{
			name:       "code already exists",
			timestamp:  graniteTime,
			codeExists: true,
			applied:    true,
		},
		{
			name:      "pre hardfork",
			timestamp: graniteTime - 1,
			applied:   false,
		},
		{
			name:      "post hardfork",
			timestamp: graniteTime + 1,
			applied:   false,
		},
		{
			name: "canyon not configured",
			override: func(cfg *params.ChainConfig) {
				cfg.GraniteTime = nil
			},
			timestamp: graniteTime,
			applied:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := params.ChainConfig{
				ChainID:     big.NewInt(2522),
				Optimism:    &params.OptimismConfig{},
				GraniteTime: &graniteTime,
			}
			if tt.override != nil {
				tt.override(&cfg)
			}
			state := &stateDb{
				codeMap: map[common.Address][]byte{
					common.HexToAddress("0xfc0000000000000000000000000000000000000a"): {0},
					common.HexToAddress("0xfc00000000000000000000000000000000000001"): make([]byte, 10000),
				},
			}
			EnsureFrxUSD(&cfg, tt.timestamp, state)
			assert.Equal(t, tt.applied, state.GetCodeSize(common.HexToAddress("0xfcc0d30000000000000000000000000000000001")) > 0)
			assert.True(t, state.GetCodeSize(common.HexToAddress("0xfcc0d30000000000000000000000000000000008")) == 0)
			if tt.applied {
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000001")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000008")), []byte(nil))
			} else {
				assert.NotEqual(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000001")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000008")), []byte(nil))
			}
		})
	}
}
