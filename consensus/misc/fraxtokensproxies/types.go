package fraxtokensproxies

import "github.com/ethereum/go-ethereum/common"

type bytecodeChange struct {
	address common.Address
	offset  uint64
	value   []byte
}

type storageChange struct {
	address     common.Address
	storageSlot common.Hash
	value       []byte
}
