package fraxtokensproxies

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func RunMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	log.Info("Holocene frax tokens migration")

	var tokensAddresses []common.Address
	var bytecodeChanges []bytecodeChange
	var storageChanges []storageChange

	proxyCode := db.GetCode(proxyCodeAddress)
	switch c.ChainID.Int64() {
	case 2521:
		tokensAddresses = devnetTokensAddresses
	case 2522:
		tokensAddresses = testnetTokensAddresses
	default:
		tokensAddresses = mainnetTokensAddresses
		bytecodeChanges = mainnetNamingBytecodeChanges
		storageChanges = mainnetNamingStorageChanges
	}

	for _, addr := range tokensAddresses {
		implementationAddress := addr
		copy(implementationAddress[:4], []byte{252, 192, 211})
		db.SetCode(implementationAddress, db.GetCode(addr))
		db.SetCode(addr, proxyCode)
		db.SetState(addr, proxyAdminSlot, common.BytesToHash(common.LeftPadBytes(proxyAdminAddress.Bytes(), common.HashLength)))
		db.SetState(addr, proxyImplementationSlot, common.BytesToHash(common.LeftPadBytes(implementationAddress.Bytes(), common.HashLength)))
	}

	for _, c := range bytecodeChanges {
		originalCode := db.GetCode(c.address)
		copy(originalCode[c.offset:], c.value)
		db.SetCode(c.address, originalCode)
	}

	for _, c := range storageChanges {
		db.SetState(c.address, c.storageSlot, common.Hash(c.value))
	}

	log.Info("Holocene frax tokens migration done")
}
