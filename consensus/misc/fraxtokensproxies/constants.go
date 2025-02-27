package fraxtokensproxies

import "github.com/ethereum/go-ethereum/common"

var mainnetTokensAddresses = []common.Address{
	common.HexToAddress("0xfc00000000000000000000000000000000000002"), // FXS
	common.HexToAddress("0xfc00000000000000000000000000000000000003"), // FPI
	common.HexToAddress("0xfc00000000000000000000000000000000000004"), // FPIS
	common.HexToAddress("0xfc00000000000000000000000000000000000005"), // sfrxETH
	common.HexToAddress("0xfc00000000000000000000000000000000000006"), // wfrxETH
	common.HexToAddress("0xfc00000000000000000000000000000000000007"), // frxBTC
}

var testnetTokensAddresses = []common.Address{
	common.HexToAddress("0xfc00000000000000000000000000000000000002"), // FXS
	common.HexToAddress("0xfc00000000000000000000000000000000000003"), // FPI
	common.HexToAddress("0xfc00000000000000000000000000000000000004"), // FPIS
	common.HexToAddress("0xfc00000000000000000000000000000000000005"), // sfrxETH
	common.HexToAddress("0xfc00000000000000000000000000000000000006"), // wfrxETH
}

var devnetTokensAddresses = []common.Address{
	common.HexToAddress("0xfc00000000000000000000000000000000000002"), // FXS
	common.HexToAddress("0xfc00000000000000000000000000000000000003"), // FPI
	common.HexToAddress("0xfc00000000000000000000000000000000000004"), // FPIS
	common.HexToAddress("0xfc00000000000000000000000000000000000005"), // sfrxETH
	common.HexToAddress("0xfc00000000000000000000000000000000000006"), // wfrxETH
	common.HexToAddress("0xfc00000000000000000000000000000000000007"), // frxBTC
}

var proxyCodeAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
var proxyAdminSlot = common.HexToHash("0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103")
var proxyImplementationSlot = common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc")
var proxyAdminAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
