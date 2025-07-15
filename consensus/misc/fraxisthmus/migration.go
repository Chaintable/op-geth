package fraxisthmus

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func RunMigration(c *params.ChainConfig, timestamp uint64, db vm.StateDB) {
	if c.ChainID.Int64() != 252 {
		return
	}

	log.Info("Isthmus frax tokens migration")

	proxyCode := db.GetCode(proxyCodeAddress)
	for _, addr := range mainnetOracles {
		implementationAddress := addr
		copy(implementationAddress[:4], []byte{252, 192, 211})
		db.SetCode(implementationAddress, db.GetCode(addr))
		db.SetCode(addr, proxyCode)
		db.SetState(addr, proxyAdminSlot, common.BytesToHash(common.LeftPadBytes(proxyAdminAddress.Bytes(), common.HashLength)))
		db.SetState(addr, proxyImplementationSlot, common.BytesToHash(common.LeftPadBytes(implementationAddress.Bytes(), common.HashLength)))
	}

	log.Info("Isthmus frax tokens migration done")
}
