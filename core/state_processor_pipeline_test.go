package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func TestPipelineGasPrice(t *testing.T) {
	const gasUsed = uint64(21_000)
	var (
		wrightTime  = uint64(1000)
		l1Fee       = big.NewInt(42_000_000) // 2000 per gas at gasUsed
		l1FeePerGas = big.NewInt(2_000)
		config      = &params.ChainConfig{WrightTime: &wrightTime}
		to          = common.HexToAddress("0xda90000000000000000000000000000000000000")
		legacyTx    = types.NewTx(&types.LegacyTx{To: &to, Gas: 60_000, GasPrice: new(big.Int)})
		depositTx   = types.NewTx(&types.DepositTx{To: &to, Gas: 1_000_000})
	)
	l1CostCalls := 0
	l1CostFunc := func(types.RollupCostData, uint64) *big.Int {
		l1CostCalls++
		return new(big.Int).Set(l1Fee)
	}

	tests := []struct {
		name      string
		blockTime uint64
		costFunc  types.L1CostFunc
		tx        *types.Transaction
		gasPrice  *big.Int
		gasUsed   uint64
		want      *big.Int
		wantCalls int
	}{
		{"zero gas price after Wright pays no L1 fee", wrightTime, l1CostFunc, legacyTx, big.NewInt(0), gasUsed, big.NewInt(0), 0},
		{"zero gas price before Wright pays the L1 fee", wrightTime - 1, l1CostFunc, legacyTx, big.NewInt(0), gasUsed, l1FeePerGas, 1},
		{"non-zero gas price after Wright pays the L1 fee", wrightTime, l1CostFunc, legacyTx, big.NewInt(1_000), gasUsed, big.NewInt(3_000), 1},
		{"deposit tx", wrightTime - 1, l1CostFunc, depositTx, big.NewInt(0), gasUsed, big.NewInt(0), 0},
		{"no gas used", wrightTime - 1, l1CostFunc, legacyTx, big.NewInt(1_000), 0, big.NewInt(1_000), 0},
		{"no L1 cost function", wrightTime - 1, nil, legacyTx, big.NewInt(1_000), gasUsed, big.NewInt(1_000), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l1CostCalls = 0
			msg := &Message{GasPrice: tt.gasPrice}
			blockCtx := vm.BlockContext{L1CostFunc: tt.costFunc, Time: tt.blockTime}
			got := pipelineGasPrice(config, blockCtx, msg, tt.tx, tt.gasUsed)
			if got.Cmp(tt.want) != 0 {
				t.Fatalf("gas price: got %v, want %v", got, tt.want)
			}
			if l1CostCalls != tt.wantCalls {
				t.Fatalf("L1 cost function calls: got %d, want %d", l1CostCalls, tt.wantCalls)
			}
			if got == msg.GasPrice {
				t.Fatal("result aliases the message gas price")
			}
		})
	}
}
