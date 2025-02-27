package misc

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/assert"
)

func TestEnsureFraxTokensProxies(t *testing.T) {
	holoceneTime := uint64(1000)
	var tests = []struct {
		name       string
		override   func(cfg *params.ChainConfig)
		timestamp  uint64
		codeExists bool
		applied    bool
	}{
		{
			name:      "at hardfork",
			timestamp: holoceneTime,
			applied:   true,
		},
		{
			name: "another chain ID",
			override: func(cfg *params.ChainConfig) {
				cfg.ChainID = big.NewInt(params.OPMainnetChainID)
			},
			timestamp: holoceneTime,
			applied:   true,
		},
		{
			name:       "code already exists",
			timestamp:  holoceneTime,
			codeExists: true,
			applied:    true,
		},
		{
			name:      "pre hardfork",
			timestamp: holoceneTime - 1,
			applied:   false,
		},
		{
			name:      "post hardfork",
			timestamp: holoceneTime + 1,
			applied:   false,
		},
		{
			name: "canyon not configured",
			override: func(cfg *params.ChainConfig) {
				cfg.HoloceneTime = nil
			},
			timestamp: holoceneTime,
			applied:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := params.ChainConfig{
				ChainID:      big.NewInt(252),
				Optimism:     &params.OptimismConfig{},
				HoloceneTime: &holoceneTime,
			}
			if tt.override != nil {
				tt.override(&cfg)
			}
			state := &stateDb{
				codeMap: map[common.Address][]byte{
					common.HexToAddress("0xfc0000000000000000000000000000000000000a"): {1, 2, 3},
					common.HexToAddress("0xfc00000000000000000000000000000000000002"): {4, 5, 6},
				},
			}
			EnsureFraxTokensProxies(&cfg, tt.timestamp, state)
			assert.Equal(t, tt.applied, state.GetCodeSize(common.HexToAddress("0xfcc0d30000000000000000000000000000000002")) > 0)
			if tt.applied {
				assert.Equal(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000002")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
			} else {
				assert.NotEqual(t, state.GetCode(common.HexToAddress("0xfc00000000000000000000000000000000000002")), state.GetCode(common.HexToAddress("0xfc0000000000000000000000000000000000000a")))
			}
		})
	}
}
