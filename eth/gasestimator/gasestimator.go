// Copyright 2023 The go-ethereum Authors
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

package gasestimator

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/internal/ethapi/override"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// Options are the contextual parameters to execute the requested call.
//
// Whilst it would be possible to pass a blockchain object that aggregates all
// these together, it would be excessively hard to test. Splitting the parts out
// allows testing without needing a proper live chain.
type Options struct {
	Config         *params.ChainConfig      // Chain configuration for hard fork selection
	Chain          core.ChainContext        // Chain context to access past block hashes
	Header         *types.Header            // Header defining the block context to execute in
	State          *state.StateDB           // Pre-state on top of which to estimate the gas
	BlockOverrides *override.BlockOverrides // Block overrides to apply during the estimation

	ErrorRatio float64 // Allowed overestimation ratio for faster estimation termination

	DefaultGasPriceForEstimate *big.Int
}

// Estimate returns the lowest possible gas limit that allows the transaction to
// run successfully with the provided context options. It returns an error if the
// transaction would always revert, or if there are unexpected failures.
func Estimate(ctx context.Context, call *core.Message, opts *Options, gasCap uint64) (uint64, []byte, error) {
	log.Info("Gas estimation started",
		"from", call.From,
		"to", call.To,
		"value", call.Value,
		"dataLen", len(call.Data),
		"callGasLimit", call.GasLimit,
		"gasCap", gasCap,
		"runMode", call.RunMode,
	)

	// Binary search the gas limit, as it may need to be higher than the amount used
	var (
		lo uint64 // lowest-known gas limit where tx execution fails
		hi uint64 // lowest-known gas limit where tx execution succeeds
	)
	// Determine the highest gas limit can be used during the estimation.
	hi = opts.Header.GasLimit
	log.Info("Gas estimation: initial hi from header", "hi", hi, "headerGasLimit", opts.Header.GasLimit)

	if call.GasLimit >= params.TxGas {
		hi = call.GasLimit
		log.Info("Gas estimation: hi updated from call.GasLimit", "hi", hi)
	}

	// Cap the maximum gas allowance according to EIP-7825 if the estimation targets Osaka
	if !opts.Config.IsOptimism() && hi > params.MaxTxGas {
		blockNumber, blockTime := opts.Header.Number, opts.Header.Time
		if opts.BlockOverrides != nil {
			if opts.BlockOverrides.Number != nil {
				blockNumber = opts.BlockOverrides.Number.ToInt()
			}
			if opts.BlockOverrides.Time != nil {
				blockTime = uint64(*opts.BlockOverrides.Time)
			}
		}
		if opts.Config.IsOsaka(blockNumber, blockTime) {
			log.Info("Gas estimation: EIP-7825 Osaka cap applied", "oldHi", hi, "newHi", params.MaxTxGas)
			hi = params.MaxTxGas
		}
	}

	// Normalize the max fee per gas the call is willing to spend.
	var feeCap *big.Int
	if call.GasFeeCap != nil {
		feeCap = call.GasFeeCap
	} else if call.GasPrice != nil {
		feeCap = call.GasPrice
	} else {
		feeCap = opts.DefaultGasPriceForEstimate
	}
	log.Info("Gas estimation: feeCap normalized", "feeCap", feeCap, "gasFeeCap", call.GasFeeCap, "gasPrice", call.GasPrice, "defaultGasPrice", opts.DefaultGasPriceForEstimate)
	// Recap the highest gas limit with account's available balance.
	if feeCap.BitLen() != 0 && call.RunMode != core.GasEstimationWithSkipCheckBalanceMode {
		balance := opts.State.GetBalance(call.From).ToBig()

		available := balance
		if call.Value != nil {
			if call.Value.Cmp(available) >= 0 {
				log.Info("Gas estimation: insufficient funds for transfer", "balance", balance, "value", call.Value)
				return 0, nil, core.ErrInsufficientFundsForTransfer
			}
			available.Sub(available, call.Value)
		}
		if opts.Config.IsCancun(opts.Header.Number, opts.Header.Time) && len(call.BlobHashes) > 0 {
			blobGasPerBlob := new(big.Int).SetInt64(params.BlobTxBlobGasPerBlob)
			blobBalanceUsage := new(big.Int).SetInt64(int64(len(call.BlobHashes)))
			blobBalanceUsage.Mul(blobBalanceUsage, blobGasPerBlob)
			blobBalanceUsage.Mul(blobBalanceUsage, call.BlobGasFeeCap)
			if blobBalanceUsage.Cmp(available) >= 0 {
				log.Info("Gas estimation: insufficient funds for blob gas", "available", available, "blobBalanceUsage", blobBalanceUsage)
				return 0, nil, core.ErrInsufficientFunds
			}
			available.Sub(available, blobBalanceUsage)
		}
		allowance := new(big.Int).Div(available, feeCap)

		// If the allowance is larger than maximum uint64, skip checking
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := call.Value
			if transfer == nil {
				transfer = new(big.Int)
			}
			log.Info("Gas estimation: capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer, "maxFeePerGas", feeCap, "fundable", allowance)
			hi = allowance.Uint64()
		}
	}
	// Recap the highest gas allowance with specified gascap.
	if gasCap != 0 && hi > gasCap {
		log.Info("Gas estimation: caller gas above allowance, capping", "requested", hi, "cap", gasCap)
		hi = gasCap
	}
	log.Info("Gas estimation: search range initialized", "lo", lo, "hi", hi)
	// If the transaction is a plain value transfer, short circuit estimation and
	// directly try 21000. Returning 21000 without any execution is dangerous as
	// some tx field combos might bump the price up even for plain transfers (e.g.
	// unused access list items). Ever so slightly wasteful, but safer overall.
	if len(call.Data) == 0 {
		if call.To != nil && opts.State.GetCodeSize(*call.To) == 0 {
			log.Info("Gas estimation: trying simple transfer fast path", "targetGas", params.TxGas)
			failed, _, err := execute(ctx, call, opts, params.TxGas)
			if !failed && err == nil {
				log.Info("Gas estimation: simple transfer fast path succeeded", "result", params.TxGas)
				return params.TxGas, nil, nil
			}
			log.Info("Gas estimation: simple transfer fast path failed, continuing with binary search", "failed", failed, "err", err)
		}
	}
	// We first execute the transaction at the highest allowable gas limit, since if this fails we
	// can return error immediately.
	log.Info("Gas estimation: executing with max gas limit", "hi", hi)
	failed, result, err := execute(ctx, call, opts, hi)
	if err != nil {
		log.Info("Gas estimation: max gas execution error", "err", err)
		return 0, nil, err
	}
	if failed {
		if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
			log.Info("Gas estimation: max gas execution failed with non-OOG error", "err", result.Err)
			return 0, result.Revert(), result.Err
		}
		log.Info("Gas estimation: gas required exceeds allowance", "hi", hi)
		return 0, nil, fmt.Errorf("gas required exceeds allowance (%d)", hi)
	}
	log.Info("Gas estimation: max gas execution succeeded", "usedGas", result.UsedGas, "maxUsedGas", result.MaxUsedGas)
	// For almost any transaction, the gas consumed by the unconstrained execution
	// above lower-bounds the gas limit required for it to succeed. One exception
	// is those that explicitly check gas remaining in order to execute within a
	// given limit, but we probably don't want to return the lowest possible gas
	// limit for these cases anyway.
	lo = result.UsedGas - 1
	log.Info("Gas estimation: lo set from usedGas", "lo", lo, "usedGas", result.UsedGas)

	// There's a fairly high chance for the transaction to execute successfully
	// with gasLimit set to the first execution's usedGas + gasRefund. Explicitly
	// check that gas amount and use as a limit for the binary search.
	optimisticGasLimit := (result.MaxUsedGas + params.CallStipend) * 64 / 63
	log.Info("Gas estimation: optimistic gas limit calculated", "optimisticGasLimit", optimisticGasLimit, "maxUsedGas", result.MaxUsedGas, "callStipend", params.CallStipend)

	if optimisticGasLimit < hi {
		log.Info("Gas estimation: trying optimistic gas limit", "optimisticGasLimit", optimisticGasLimit)
		failed, _, err = execute(ctx, call, opts, optimisticGasLimit)
		if err != nil {
			// This should not happen under normal conditions since if we make it this far the
			// transaction had run without error at least once before.
			log.Error("Execution error in estimate gas", "err", err)
			return 0, nil, err
		}
		if failed {
			lo = optimisticGasLimit
			log.Info("Gas estimation: optimistic execution failed, updated lo", "lo", lo)
		} else {
			hi = optimisticGasLimit
			log.Info("Gas estimation: optimistic execution succeeded, updated hi", "hi", hi)
		}
	}
	// Binary search for the smallest gas limit that allows the tx to execute successfully.
	log.Info("Gas estimation: starting binary search", "lo", lo, "hi", hi)
	iteration := 0
	for lo+1 < hi {
		iteration++
		if opts.ErrorRatio > 0 {
			// It is a bit pointless to return a perfect estimation, as changing
			// network conditions require the caller to bump it up anyway. Since
			// wallets tend to use 20-25% bump, allowing a small approximation
			// error is fine (as long as it's upwards).
			errorRatio := float64(hi-lo) / float64(hi)
			if errorRatio < opts.ErrorRatio {
				log.Info("Gas estimation: early termination due to error ratio", "iteration", iteration, "lo", lo, "hi", hi, "errorRatio", errorRatio, "threshold", opts.ErrorRatio)
				break
			}
		}
		mid := lo + (hi-lo)/2
		originalMid := mid
		if mid > lo*2 {
			// Most txs don't need much higher gas limit than their gas used, and most txs don't
			// require near the full block limit of gas, so the selection of where to bisect the
			// range here is skewed to favor the low side.
			mid = lo * 2
		}
		log.Info("Gas estimation: binary search iteration", "iteration", iteration, "lo", lo, "hi", hi, "originalMid", originalMid, "adjustedMid", mid)

		failed, _, err = execute(ctx, call, opts, mid)
		if err != nil {
			// This should not happen under normal conditions since if we make it this far the
			// transaction had run without error at least once before.
			log.Error("Execution error in estimate gas", "err", err)
			return 0, nil, err
		}
		if failed {
			lo = mid
			log.Info("Gas estimation: binary search iteration failed, updated lo", "iteration", iteration, "lo", lo)
		} else {
			hi = mid
			log.Info("Gas estimation: binary search iteration succeeded, updated hi", "iteration", iteration, "hi", hi)
		}
	}
	log.Info("Gas estimation: binary search completed", "iterations", iteration, "result", hi)
	return hi, nil, nil
}

// execute is a helper that executes the transaction under a given gas limit and
// returns true if the transaction fails for a reason that might be related to
// not enough gas. A non-nil error means execution failed due to reasons unrelated
// to the gas limit.
func execute(ctx context.Context, call *core.Message, opts *Options, gasLimit uint64) (bool, *core.ExecutionResult, error) {
	// Configure the call for this specific execution (and revert the change after)
	defer func(gas uint64) { call.GasLimit = gas }(call.GasLimit)
	call.GasLimit = gasLimit

	// Execute the call and separate execution faults caused by a lack of gas or
	// other non-fixable conditions
	result, err := run(ctx, call, opts)
	if err != nil {
		if errors.Is(err, core.ErrIntrinsicGas) || errors.Is(err, core.ErrInsufficientGasForL1Cost) {
			return true, nil, nil // Special case, raise gas limit
		}
		if errors.Is(err, core.ErrGasLimitTooHigh) {
			return true, nil, nil // Special case, lower gas limit
		}
		return true, nil, err // Bail out
	}
	return result.Failed(), result, nil
}

// run assembles the EVM as defined by the consensus rules and runs the requested
// call invocation.
func run(ctx context.Context, call *core.Message, opts *Options) (*core.ExecutionResult, error) {
	// Assemble the call and the call context
	var (
		evmContext = core.NewEVMBlockContext(opts.Header, opts.Chain, nil, opts.Config, opts.State)
		dirtyState = opts.State.Copy()
	)
	if opts.BlockOverrides != nil {
		if err := opts.BlockOverrides.Apply(&evmContext); err != nil {
			return nil, err
		}
	}
	// Lower the basefee to 0 to avoid breaking EVM
	// invariants (basefee < feecap).
	if call.GasPrice.Sign() == 0 {
		evmContext.BaseFee = new(big.Int)
	}
	if call.BlobGasFeeCap != nil && call.BlobGasFeeCap.BitLen() == 0 {
		evmContext.BlobBaseFee = new(big.Int)
	}
	evm := vm.NewEVM(evmContext, dirtyState, opts.Config, vm.Config{NoBaseFee: true})

	// Monitor the outer context and interrupt the EVM upon cancellation. To avoid
	// a dangling goroutine until the outer estimation finishes, create an internal
	// context for the lifetime of this method call.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()
	// Execute the call, returning a wrapped error or the result
	result, err := core.ApplyMessage(evm, call, new(core.GasPool).AddGas(core.DefaultMantleBlockGasLimit))
	if vmerr := dirtyState.Error(); vmerr != nil {
		return nil, vmerr
	}
	if err != nil {
		return result, fmt.Errorf("failed with %d gas: %w", call.GasLimit, err)
	}
	return result, nil
}
