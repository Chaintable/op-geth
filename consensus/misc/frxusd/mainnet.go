package frxusd

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func RunMainnetMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	log.Info("Getting frxUSD/sfrxUSD proxy code", "address", frxUSDMigrationProxyCodeAddress)
	proxyCode := db.GetCode(frxUSDMigrationProxyCodeAddress)
	log.Info("Moving frxUSD implementation", "from", frxUSDAddress, "to", frxUSDImplementationAddress)
	frxUSDImplementationCode := db.GetCode(frxUSDAddress)
	for _, i := range frxUSDL1TokenReplacementIndexes {
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

	log.Info("Moving sfrxUSD implementation", "from", sfrxUSDAddress, "to", sfrxUSDImplementationAddress)
	sfrxUSDImplementationCode := db.GetCode(sfrxUSDAddress)
	for _, i := range sfrxUSDL1TokenReplacementIndexes {
		copy(sfrxUSDImplementationCode[i:], sfrxUSDL1Token)
	}
	db.SetCode(sfrxUSDImplementationAddress, sfrxUSDImplementationCode)
	log.Info("Setting sfrxUSD proxy", "address", frxUSDAddress)
	db.SetCode(sfrxUSDAddress, proxyCode)
	log.Info("Setting sfrxUSD proxy and storage", "address", sfrxUSDAddress)
	db.SetState(sfrxUSDAddress, frxUSDProxyAdminSlot, common.BytesToHash(common.LeftPadBytes(frxUSDProxyAdminAddress.Bytes(), common.HashLength)))
	db.SetState(sfrxUSDAddress, frxUSDProxyImplementationSlot, common.BytesToHash(common.LeftPadBytes(sfrxUSDImplementationAddress.Bytes(), common.HashLength)))
	db.SetState(sfrxUSDAddress, common.HexToHash("0x03"), sfrxUSDNameBytes)
	db.SetState(sfrxUSDAddress, common.HexToHash("0x04"), sfrxUSDSymbolBytes)
}
