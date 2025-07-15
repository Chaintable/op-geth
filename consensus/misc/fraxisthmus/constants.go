package fraxisthmus

import "github.com/ethereum/go-ethereum/common"

var mainnetOracles = []common.Address{
	common.HexToAddress("0xf750636e1df115e3b334ed06e5b45c375107fc60"),
	common.HexToAddress("0x1B680F4385f24420D264D78cab7C58365ED3F1FF"),
}

var proxyCodeAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
var proxyAdminAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
var proxyAdminSlot = common.HexToHash("0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103")
var proxyImplementationSlot = common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc")
