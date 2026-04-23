package eth

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
)

const nativeAddr = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

var (
	abiUint8, _   = abi.NewType("uint8", "", nil)
	abiUint256, _ = abi.NewType("uint256", "", nil)
	abiString, _  = abi.NewType("string", "", nil)
	abiAddress, _ = abi.NewType("address", "", nil)

	erc20ABI = abi.ABI{
		Methods: map[string]abi.Method{
			"name":        funcName,
			"symbol":      funcSymbol,
			"decimals":    funcDecimals,
			"totalSupply": funcTotalSupply,
			"balanceOf":   funcBalanceOf,
		},
	}

	funcName = abi.NewMethod("name", "name", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{{Name: "", Type: abiString, Indexed: false}},
	)
	funcSymbol = abi.NewMethod("symbol", "symbol", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{{Name: "", Type: abiString, Indexed: false}},
	)
	funcDecimals = abi.NewMethod("decimals", "decimals", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{{Name: "", Type: abiUint8, Indexed: false}},
	)
	funcTotalSupply = abi.NewMethod("totalSupply", "totalSupply", abi.Function, "", false, false,
		[]abi.Argument{},
		[]abi.Argument{{Name: "", Type: abiUint256, Indexed: false}},
	)
	funcBalanceOf = abi.NewMethod("balanceOf", "balanceOf", abi.Function, "", false, false,
		[]abi.Argument{{Name: "", Type: abiAddress, Indexed: false}},
		[]abi.Argument{{Name: "", Type: abiUint256, Indexed: false}},
	)
)

func handleNative(statedb *state.StateDB, data []byte) ([]byte, int, error) {
	method, err := erc20ABI.MethodById(data)
	if err != nil {
		return nil, errNativeMethodNotFound, err
	}
	switch method.Name {
	case "name", "symbol":
		res, err := method.Outputs.Pack("ETH")
		if err != nil {
			return nil, errNativeMethodOutput, err
		}
		return res, 0, nil
	case "decimals":
		res, err := method.Outputs.Pack(uint8(18))
		if err != nil {
			return nil, errNativeMethodOutput, err
		}
		return res, 0, nil
	case "totalSupply":
		res, err := method.Outputs.Pack(big.NewInt(1_000_000_000_000_000_000))
		if err != nil {
			return nil, errNativeMethodOutput, err
		}
		return res, 0, nil
	case "balanceOf":
		inputs, err := method.Inputs.Unpack(data[4:])
		if err != nil || len(inputs) == 0 {
			return nil, errNativeMethodInput, fmt.Errorf("input address error")
		}
		address, ok := inputs[0].(common.Address)
		if !ok {
			return nil, errNativeMethodInputAddress, fmt.Errorf("input address error")
		}
		balance, err := method.Outputs.Pack(statedb.GetBalance(address).ToBig())
		if err != nil {
			return nil, errNativeMethodOutput, err
		}
		if statedb.Error() != nil {
			return nil, errNativeMethodStateError, statedb.Error()
		}
		return balance, 0, nil
	default:
		return nil, errNativeMethodNotFound, fmt.Errorf("method not found")
	}
}

const (
	errNativeMethodNotFound     = -40001
	errNativeMethodInput        = -40002
	errNativeMethodInputAddress = -40003
	errNativeMethodOutput       = -40010
	errNativeMethodStateError   = -40011
	errMessageExecuting         = -40012
)
