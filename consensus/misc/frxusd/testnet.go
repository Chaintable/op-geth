package frxusd

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func RunTestnetMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	log.Info("Getting frxUSD proxy code", "address", frxUSDMigrationProxyCodeAddress)
	proxyCode := db.GetCode(frxUSDMigrationProxyCodeAddress)
	log.Info("Moving frxUSD implementation", "from", frxUSDAddress, "to", frxUSDImplementationAddress)
	frxUSDImplementationCode := db.GetCode(frxUSDAddress)
	for _, i := range testnetFrxUSDL1TokenReplacementIndexes {
		copy(frxUSDImplementationCode[i:], frxUSDL1Token)
	}
	db.SetCode(frxUSDImplementationAddress, frxUSDImplementationCode)
	log.Info("Setting frxUSD proxy", "address", frxUSDAddress)
	db.SetCode(frxUSDAddress, proxyCode)
	log.Info("Setting frxUSD storage variables", "address", frxUSDAddress)
	db.SetState(frxUSDAddress, frxUSDProxyAdminSlot, common.BytesToHash(common.LeftPadBytes(frxUSDProxyAdminAddress.Bytes(), common.HashLength)))
	db.SetState(frxUSDAddress, frxUSDProxyImplementationSlot, common.BytesToHash(common.LeftPadBytes(frxUSDImplementationAddress.Bytes(), common.HashLength)))
	db.SetState(frxUSDAddress, common.HexToHash("0x03"), frxUSDNameBytes)
	db.SetState(frxUSDAddress, common.HexToHash("0x04"), frxUSDSymbolBytes)
}
