package frxusd

import "github.com/ethereum/go-ethereum/common"

var frxUSDMigrationProxyCodeAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
var frxUSDProxyAdminAddress = common.HexToAddress("0xfc0000000000000000000000000000000000000a")
var frxUSDProxyAdminSlot = common.HexToHash("0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103")
var frxUSDProxyImplementationSlot = common.HexToHash("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc")

var frxUSDAddress = common.HexToAddress("0xfc00000000000000000000000000000000000001")
var frxUSDImplementationAddress = common.HexToAddress("0xfcc0d30000000000000000000000000000000001")
var frxUSDNameBytes = common.HexToHash("0x4672617820555344000000000000000000000000000000000000000000000010")   // Frax USD
var frxUSDSymbolBytes = common.HexToHash("0x667278555344000000000000000000000000000000000000000000000000000c") // frxUSD
var frxUSDL1Token = common.FromHex("0xCAcd6fd266aF91b8AeD52aCCc382b4e165586E29")

var sfrxUSDAddress = common.HexToAddress("0xfc00000000000000000000000000000000000008")
var sfrxUSDImplementationAddress = common.HexToAddress("0xfcc0d30000000000000000000000000000000008")
var sfrxUSDNameBytes = common.HexToHash("0x5374616b6564204672617820555344000000000000000000000000000000001e")   // Staked Frax USD
var sfrxUSDSymbolBytes = common.HexToHash("0x736672785553440000000000000000000000000000000000000000000000000e") // sfrxUSD
var sfrxUSDL1Token = common.FromHex("0xcf62F905562626CfcDD2261162a51fd02Fc9c5b6")

var frxUSDL1TokenReplacementIndexes = []uint{2137, 6850, 7218}
var sfrxUSDL1TokenReplacementIndexes = []uint{2137, 6850, 7218}

var testnetFrxUSDL1TokenReplacementIndexes = []uint{2137, 6962, 7330}

var devnetFrxUSDL1TokenReplacementIndexes = []uint{693, 1303}
var devnetSfrxUSDL1TokenReplacementIndexes = []uint{693, 1303}
