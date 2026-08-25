package processor

import (
	"github.com/ethereum/go-ethereum/common"
)

// BlockchainReader provides read access to blockchain headers for reorg computation.
type BlockchainReader interface {
	// GetHeaderByHash retrieves a block header by its hash.
	GetHeaderByHash(hash common.Hash) Header
}

// Header represents a minimal block header interface needed for reorg detection.
type Header interface {
	Number() uint64
	Hash() common.Hash
	ParentHash() common.Hash
	Time() uint64
}
