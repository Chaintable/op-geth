// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

func TestCalcRefund(t *testing.T) {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	statedb.AddRefund(1000000)
	evm := vm.NewEVM(vm.BlockContext{BlockNumber: big.NewInt(100)}, statedb, params.OptimismTestConfig, vm.Config{})
	gp := NewGasPool(200000000000)
	st := newStateTransition(evm, &Message{}, gp)
	st.initialBudget = vm.NewGasBudget(10000000000)
	st.gasRemaining = vm.NewGasBudget(500000)

	// gasUsed = st.initialGas - st.gasRemaining = 10000000000 - 500000 = 9999500000
	// maxRefund = gasUsed/5 = 2000000/5 = 1999900000
	// st.state.GetRefund() = 1000000 < maxRefund, so return 1000000
	if refund := st.calcRefundPreArsia(4000, false); refund != 1000000 {
		t.Errorf("before skadi calc refund is: %d, expectd: %v", refund, 1000000)
	}

	// gasUsed = st.initialGas/tokeRatio - st.gasRemaining = 10000000000/4000 - 500000 = 2000000
	// maxRefund = gasUsed/5 = 2000000/5 = 400000
	// st.state.GetRefund() = 1000000 > maxRefund, so return 400000
	if refund := st.calcRefundPreArsia(4000, true); refund != 400000 {
		t.Errorf("after skadi calc refund is: %d, expectd: %v", refund, 400000)
	}
}

func TestDepositGasPoolReservation(t *testing.T) {
	mkSt := func(gp *GasPool, msg *Message) *stateTransition {
		statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
		// Regolith activates at time=0 in OptimismTestConfig, so the system-tx
		// branch returns ErrSystemTxNotSupported as in production.
		evm := vm.NewEVM(
			vm.BlockContext{BlockNumber: big.NewInt(100), Time: 1_000_000},
			statedb,
			params.OptimismTestConfig,
			vm.Config{},
		)
		return newStateTransition(evm, msg, gp)
	}

	t.Run("deposit reserves GasLimit when pool is sufficient", func(t *testing.T) {
		gp := NewGasPool(30_000_000)
		msg := &Message{IsDepositTx: true, GasLimit: 2_000_000}
		st := mkSt(gp, msg)

		if _, err := st.preCheck(); err != nil {
			t.Fatalf("preCheck err: %v", err)
		}
		if got := gp.Gas(); got != 28_000_000 {
			t.Fatalf("gp.Gas() after deposit: got %d, want 28_000_000", got)
		}
	})

	t.Run("deposit fails when pool is insufficient", func(t *testing.T) {
		gp := NewGasPool(30_000_000)
		msg := &Message{IsDepositTx: true, GasLimit: 30_000_001}
		st := mkSt(gp, msg)

		_, err := st.preCheck()
		if !errors.Is(err, ErrGasLimitReached) {
			t.Fatalf("preCheck err: got %v, want ErrGasLimitReached", err)
		}
		if got := gp.Gas(); got != 30_000_000 {
			t.Fatalf("gp mutated after failed SubGas: got %d, want 30_000_000", got)
		}
	})

	t.Run("system tx must not touch the gas pool", func(t *testing.T) {
		gp := NewGasPool(30_000_000)
		msg := &Message{IsDepositTx: true, IsSystemTx: true, GasLimit: 1_000_000}
		st := mkSt(gp, msg)

		// Post-Regolith system txs return ErrSystemTxNotSupported before
		// touching gp. We only assert gp is untouched.
		_, _ = st.preCheck()
		if got := gp.Gas(); got != 30_000_000 {
			t.Fatalf("system tx mutated gp: got %d, want 30_000_000", got)
		}
	})

	t.Run("multiple deposits drain pool cumulatively", func(t *testing.T) {
		gp := NewGasPool(30_000_000)
		for i, gl := range []uint64{500_000, 1_500_000, 3_000_000} {
			msg := &Message{IsDepositTx: true, GasLimit: gl}
			st := mkSt(gp, msg)
			if _, err := st.preCheck(); err != nil {
				t.Fatalf("deposit %d preCheck err: %v", i, err)
			}
		}
		// 30M - 0.5M - 1.5M - 3M = 25M
		if got := gp.Gas(); got != 25_000_000 {
			t.Fatalf("cumulative gp: got %d, want 25_000_000", got)
		}
	})
}

func TestFloorDataGas(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	key1 := common.HexToHash("0xaa")
	key2 := common.HexToHash("0xbb")

	tests := []struct {
		name       string
		amsterdam  bool
		data       []byte
		accessList types.AccessList
		want       uint64
	}{
		{
			name: "pre-amsterdam/empty",
			want: params.TxGas,
		},
		{
			name: "pre-amsterdam/zero-bytes-only",
			data: bytes.Repeat([]byte{0x00}, 100),
			// 100 zero tokens * 10 cost = 1000
			want: params.TxGas + 100*params.TxCostFloorPerToken,
		},
		{
			name: "pre-amsterdam/non-zero-bytes-only",
			data: bytes.Repeat([]byte{0xff}, 100),
			// 100 nz * 4 tokens * 10 cost = 4000
			want: params.TxGas + 100*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken,
		},
		{
			name: "pre-amsterdam/mixed",
			data: append(bytes.Repeat([]byte{0x00}, 50), bytes.Repeat([]byte{0xff}, 50)...),
			// 50 zero + 50*4 nz = 250 tokens * 10 = 2500
			want: params.TxGas + (50+50*params.TxTokenPerNonZeroByte)*params.TxCostFloorPerToken,
		},
		{
			name: "pre-amsterdam/access-list-ignored",
			data: bytes.Repeat([]byte{0xff}, 10),
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1, key2}},
			},
			// pre-amsterdam: floor calculation does not include access list
			want: params.TxGas + 10*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken,
		},
		{
			name:      "amsterdam/empty",
			amsterdam: true,
			want:      params.TxGas,
		},
		{
			name:      "amsterdam/data-only",
			amsterdam: true,
			data:      bytes.Repeat([]byte{0x00}, 1024),
			// post-amsterdam: every byte = 4 tokens regardless of value
			want: params.TxGas + 1024*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken7976,
		},
		{
			name:      "amsterdam/data-non-zero",
			amsterdam: true,
			data:      bytes.Repeat([]byte{0xff}, 1024),
			// same as zero data post-amsterdam
			want: params.TxGas + 1024*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken7976,
		},
		{
			name:      "amsterdam/access-list-addresses-only",
			amsterdam: true,
			accessList: types.AccessList{
				{Address: addr1},
				{Address: addr2},
			},
			// 2 * 20 bytes * 4 tokens/byte * 16 cost/token
			want: params.TxGas + 2*common.AddressLength*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken7976,
		},
		{
			name:      "amsterdam/access-list-with-storage-keys",
			amsterdam: true,
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1, key2}},
			},
			// 1 addr * 20 * 4 + 2 keys * 32 * 4 = 80 + 256 = 336 tokens * 16
			want: params.TxGas + (1*common.AddressLength+2*common.HashLength)*params.TxTokenPerNonZeroByte*params.TxCostFloorPerToken7976,
		},
		{
			name:      "amsterdam/mixed",
			amsterdam: true,
			data:      bytes.Repeat([]byte{0xff}, 100),
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1}},
				{Address: addr2, StorageKeys: []common.Hash{key1, key2}},
			},
			// data: 100*4 = 400; addrs: 2*20*4 = 160; keys: 3*32*4 = 384; total = 944 * 16
			want: params.TxGas + (100*params.TxTokenPerNonZeroByte+2*common.AddressLength*params.TxTokenPerNonZeroByte+3*common.HashLength*params.TxTokenPerNonZeroByte)*params.TxCostFloorPerToken7976,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules := params.Rules{IsAmsterdam: tt.amsterdam}
			got, err := FloorDataGas(rules, tt.data, tt.accessList)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("gas mismatch: got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestIntrinsicGas(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	key1 := common.HexToHash("0xaa")
	key2 := common.HexToHash("0xbb")

	const (
		amsterdamAddressCost    = uint64(common.AddressLength) * params.TxCostFloorPerToken7976 * params.TxTokenPerNonZeroByte // 1280
		amsterdamStorageKeyCost = uint64(common.HashLength) * params.TxCostFloorPerToken7976 * params.TxTokenPerNonZeroByte    // 2048
	)

	tests := []struct {
		name        string
		data        []byte
		accessList  types.AccessList
		authList    []types.SetCodeAuthorization
		creation    bool
		isHomestead bool
		isEIP2028   bool
		isEIP3860   bool
		isAmsterdam bool
		want        uint64
	}{
		{
			name: "frontier/empty-call",
			want: params.TxGas,
		},
		{
			name:        "frontier/contract-creation-pre-homestead",
			creation:    true,
			isHomestead: false,
			// pre-homestead, contract creation still uses TxGas
			want: params.TxGas,
		},
		{
			name:        "homestead/contract-creation",
			creation:    true,
			isHomestead: true,
			want:        params.TxGasContractCreation,
		},
		{
			name: "frontier/non-zero-data",
			data: bytes.Repeat([]byte{0xff}, 100),
			// 100 nz bytes * 68 (frontier)
			want: params.TxGas + 100*params.TxDataNonZeroGasFrontier,
		},
		{
			name:      "istanbul/non-zero-data",
			data:      bytes.Repeat([]byte{0xff}, 100),
			isEIP2028: true,
			// 100 nz bytes * 16 (post-EIP2028)
			want: params.TxGas + 100*params.TxDataNonZeroGasEIP2028,
		},
		{
			name:      "istanbul/zero-data",
			data:      bytes.Repeat([]byte{0x00}, 100),
			isEIP2028: true,
			// 100 zero bytes * 4
			want: params.TxGas + 100*params.TxDataZeroGas,
		},
		{
			name:      "istanbul/mixed-data",
			data:      append(bytes.Repeat([]byte{0x00}, 50), bytes.Repeat([]byte{0xff}, 50)...),
			isEIP2028: true,
			want:      params.TxGas + 50*params.TxDataZeroGas + 50*params.TxDataNonZeroGasEIP2028,
		},
		{
			name:        "shanghai/init-code-word-gas",
			data:        bytes.Repeat([]byte{0x00}, 64), // 2 words
			creation:    true,
			isHomestead: true,
			isEIP2028:   true,
			isEIP3860:   true,
			// TxGasContractCreation + 64 zero bytes * 4 + 2 words * 2
			want: params.TxGasContractCreation + 64*params.TxDataZeroGas + 2*params.InitCodeWordGas,
		},
		{
			name:        "shanghai/init-code-non-multiple-of-32",
			data:        bytes.Repeat([]byte{0x00}, 33), // 2 words (rounded up)
			creation:    true,
			isHomestead: true,
			isEIP2028:   true,
			isEIP3860:   true,
			want:        params.TxGasContractCreation + 33*params.TxDataZeroGas + 2*params.InitCodeWordGas,
		},
		{
			name: "berlin/access-list",
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1, key2}},
				{Address: addr2, StorageKeys: []common.Hash{key1}},
			},
			isEIP2028: true,
			// 2 addrs * 2400 + 3 keys * 1900
			want: params.TxGas + 2*params.TxAccessListAddressGas + 3*params.TxAccessListStorageKeyGas,
		},
		{
			name: "amsterdam/access-list-extra-cost",
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1, key2}},
				{Address: addr2, StorageKeys: []common.Hash{key1}},
			},
			isEIP2028:   true,
			isAmsterdam: true,
			// base access-list charge + EIP-7981 extra
			want: params.TxGas +
				2*params.TxAccessListAddressGas + 3*params.TxAccessListStorageKeyGas +
				2*amsterdamAddressCost + 3*amsterdamStorageKeyCost,
		},
		{
			name: "prague/auth-list",
			authList: []types.SetCodeAuthorization{
				{Address: addr1},
				{Address: addr2},
				{Address: addr1},
			},
			isEIP2028: true,
			// 3 auths * 25000
			want: params.TxGas + 3*params.CallNewAccountGas,
		},
		{
			name: "amsterdam/combined",
			data: bytes.Repeat([]byte{0xff}, 100),
			accessList: types.AccessList{
				{Address: addr1, StorageKeys: []common.Hash{key1}},
			},
			authList: []types.SetCodeAuthorization{
				{Address: addr2},
			},
			isEIP2028:   true,
			isAmsterdam: true,
			want: params.TxGas +
				100*params.TxDataNonZeroGasEIP2028 +
				1*params.TxAccessListAddressGas + 1*params.TxAccessListStorageKeyGas +
				1*amsterdamAddressCost + 1*amsterdamStorageKeyCost +
				1*params.CallNewAccountGas,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IntrinsicGas(tt.data, tt.accessList, tt.authList,
				tt.creation, tt.isHomestead, tt.isEIP2028, tt.isEIP3860, tt.isAmsterdam)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := vm.GasCosts{RegularGas: tt.want}
			if got != want {
				t.Fatalf("gas mismatch: got %+v, want %+v", got, want)
			}
		})
	}
}

// TestDepositFailedBVMETHTransferGasPool covers a deposit whose BVM_ETH transfer fails
// (ErrEthTxValueTooLarge) before the EVM runs. The deposit must still be force-included and
// recorded as using all of its gas, and the gas pool must hold the matching reservation.
// Before preCheck was moved ahead of transferBVMETH the pool was never debited while the
// receipt and cumulative counters still grew by GasLimit.
func TestDepositFailedBVMETHTransferGasPool(t *testing.T) {
	cfg := *params.OptimismTestConfig
	zero := uint64(0)
	cfg.BVMETHMintUpgradeTime = &zero // production Mantle runs with the BVM_ETH transfer path active

	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	evm := vm.NewEVM(
		vm.BlockContext{BlockNumber: big.NewInt(100), Time: 1_000_000, BaseFee: big.NewInt(0)},
		statedb,
		&cfg,
		vm.Config{},
	)
	const poolGas, depositGas = uint64(30_000_000), uint64(2_000_000)
	to := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	msg := &Message{
		From:        common.HexToAddress("0x00000000000000000000000000000000000000bb"),
		To:          &to,
		IsDepositTx: true,
		GasLimit:    depositGas,
		Value:       new(uint256.Int),
		GasPrice:    new(uint256.Int),
		GasFeeCap:   new(uint256.Int),
		GasTipCap:   new(uint256.Int),
		ETHTxValue:  big.NewInt(1), // From holds no BVM_ETH, so the transfer fails
	}

	t.Run("failed transfer keeps pool and counters consistent", func(t *testing.T) {
		gp := NewGasPool(poolGas)
		st := newStateTransition(evm, msg, gp)

		res, err := st.execute()
		if err != nil {
			t.Fatalf("execute: unexpected consensus error: %v", err)
		}
		if res == nil || !errors.Is(res.Err, ErrEthTxValueTooLarge) {
			t.Fatalf("expected failed deposit with ErrEthTxValueTooLarge, got %+v", res)
		}
		if res.UsedGas != depositGas {
			t.Fatalf("failed deposit UsedGas: got %d, want %d", res.UsedGas, depositGas)
		}
		if got := gp.Gas(); got != poolGas-depositGas {
			t.Fatalf("gas pool remaining: got %d, want %d (GasLimit must be reserved)", got, poolGas-depositGas)
		}
		if got := gp.CumulativeUsed(); got != depositGas {
			t.Fatalf("gas pool cumulativeUsed: got %d, want %d", got, depositGas)
		}
	})

	t.Run("deposit is not force-included when the pool cannot cover GasLimit", func(t *testing.T) {
		gp := NewGasPool(depositGas - 1)
		st := newStateTransition(evm, msg, gp)

		if _, err := st.execute(); !errors.Is(err, ErrGasLimitReached) {
			t.Fatalf("expected ErrGasLimitReached, got %v", err)
		}
		if got := gp.Gas(); got != depositGas-1 {
			t.Fatalf("pool mutated after rejected deposit: got %d, want %d", got, depositGas-1)
		}
		if got := gp.CumulativeUsed(); got != 0 {
			t.Fatalf("cumulativeUsed mutated after rejected deposit: got %d", got)
		}
	})

}

type depositGasTestChain struct {
	consensus.ChainHeaderReader
	config *params.ChainConfig
	engine consensus.Engine
}

func (c depositGasTestChain) Config() *params.ChainConfig { return c.config }
func (c depositGasTestChain) Engine() consensus.Engine    { return c.engine }

func TestFailedBVMETHDepositBlockGas(t *testing.T) {
	zero := uint64(0)
	config := *params.TestChainConfig
	config.Optimism = params.OptimismTestConfig.Optimism
	config.BedrockBlock = big.NewInt(0)
	config.RegolithTime = &zero
	config.BVMETHMintUpgradeTime = &zero

	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatal(err)
	}
	from := common.HexToAddress("0x01")
	to := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	const depositGas = uint64(21_000)
	txs := []*types.Transaction{
		types.NewTx(&types.DepositTx{From: from, To: &to, Value: big.NewInt(0), Gas: depositGas, SourceHash: common.HexToHash("0x01")}),
		types.NewTx(&types.DepositTx{From: from, To: &to, Value: big.NewInt(0), Gas: depositGas, EthTxValue: big.NewInt(1), SourceHash: common.HexToHash("0x02")}),
	}
	header := &types.Header{Number: big.NewInt(1), Time: 1, GasLimit: 30_000_000, BaseFee: big.NewInt(1), Difficulty: big.NewInt(0)}
	block := types.NewBlockWithHeader(header).WithBody(types.Body{Transactions: txs})
	chain := depositGasTestChain{config: &config, engine: beacon.New(ethash.NewFaker())}
	result, err := NewStateProcessor(chain).Process(context.Background(), block, statedb, vm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Receipts) != 2 || result.Receipts[0].Status != types.ReceiptStatusSuccessful || result.Receipts[1].Status != types.ReceiptStatusFailed {
		t.Fatalf("unexpected deposit receipts: %+v", result.Receipts)
	}
	if result.Receipts[1].GasUsed != depositGas || result.Receipts[1].CumulativeGasUsed != 2*depositGas || result.GasUsed != 2*depositGas {
		t.Fatalf("failed deposit gas: receipt=%d cumulative=%d block=%d, want %d/%d/%d",
			result.Receipts[1].GasUsed, result.Receipts[1].CumulativeGasUsed, result.GasUsed,
			depositGas, 2*depositGas, 2*depositGas)
	}
	header.GasUsed = result.Receipts[1].CumulativeGasUsed
	header.Bloom = types.MergeBloom(result.Receipts)
	remote := types.NewBlockWithHeader(header).WithBody(types.Body{Transactions: txs})
	if err := NewBlockValidator(&config, nil).ValidateState(remote, statedb, result, true); err != nil {
		t.Fatalf("block with receipt-derived gas was rejected: %v", err)
	}
}
