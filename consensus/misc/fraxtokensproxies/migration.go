package fraxtokensproxies

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func RunMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	log.Info("Migrating frax tokens to proxies")

	var tokensAddresses []common.Address
	proxyCode := db.GetCode(proxyCodeAddress)
	switch c.ChainID.Int64() {
	case 2521:
		tokensAddresses = devnetTokensAddresses
	case 2522:
		tokensAddresses = testnetTokensAddresses
	default:
		tokensAddresses = mainnetTokensAddresses
	}
	for _, addr := range tokensAddresses {
		implementationAddress := addr
		copy(implementationAddress[:4], []byte{252, 192, 211})
		db.SetCode(implementationAddress, db.GetCode(addr))
		db.SetCode(addr, proxyCode)
		db.SetState(addr, proxyAdminSlot, common.BytesToHash(common.LeftPadBytes(proxyAdminAddress.Bytes(), common.HashLength)))
		db.SetState(addr, proxyImplementationSlot, common.BytesToHash(common.LeftPadBytes(implementationAddress.Bytes(), common.HashLength)))
	}

	log.Info("Migration of frax tokens complete")
}
