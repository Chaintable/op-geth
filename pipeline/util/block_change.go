package util

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/pipeline/types"
)

// HeaderFetcher is an interface for fetching block headers by hash
type HeaderFetcher func(hash common.Hash) *types.BlockContext

// ComputeBlockChange computes the block change between two blocks
// Returns nil if no change, otherwise returns a BlockChangeNotification
func ComputeBlockChange(lastPushedBlock, currentBlock types.BlockContext, fetcher HeaderFetcher) *types.BlockChangeNotification {
	if lastPushedBlock.BlockNumber > currentBlock.BlockNumber {
		return nil
	}

	_, dropBlocks, newBlocks := getCommonAncestor(lastPushedBlock, currentBlock, fetcher)

	if len(dropBlocks) > 0 {
		// Fork/reorg case
		return &types.BlockChangeNotification{
			ChangeType: 2,
			NewBlocks:  newBlocks,
			DropBlocks: dropBlocks,
		}
	} else if len(newBlocks) > 0 {
		// Normal case: new blocks on canonical chain
		return &types.BlockChangeNotification{
			ChangeType: 1,
			NewBlocks:  newBlocks,
		}
	}

	return nil
}

// getCommonAncestor returns the common ancestor and the paths from it to both blocks
func getCommonAncestor(blocka, blockb types.BlockContext, fetcher HeaderFetcher) (types.BlockContext, []types.BlockContext, []types.BlockContext) {
	var (
		chainA, chainB []types.BlockContext
	)

	// Fast path: blockb is direct child of blocka
	if blockb.ParentHash == blocka.Hash {
		return blocka, chainA, []types.BlockContext{blockb}
	}

	// Bring blockb down to same height as blocka
	for blockb.BlockNumber > blocka.BlockNumber {
		chainB = append(chainB, blockb)
		headerb := fetcher(blockb.ParentHash)
		if headerb == nil {
			log.Crit("Failed to get header by hash", "hash", blockb.ParentHash)
		}
		blockb = *headerb
	}

	// Walk both chains back until we find common ancestor
	for blocka.Hash != blockb.Hash {
		chainA = append(chainA, blocka)
		headera := fetcher(blocka.ParentHash)
		if headera == nil {
			log.Crit("Failed to get header by hash", "hash", blocka.ParentHash)
		}
		blocka = *headera

		chainB = append(chainB, blockb)
		headerb := fetcher(blockb.ParentHash)
		if headerb == nil {
			log.Crit("Failed to get header by hash", "hash", blockb.ParentHash)
		}
		blockb = *headerb
	}

	// Now blocka == blockb == ancestor
	// Reverse chains so they're in ascending order
	for i, j := 0, len(chainA)-1; i < j; i, j = i+1, j-1 {
		chainA[i], chainA[j] = chainA[j], chainA[i]
	}
	for i, j := 0, len(chainB)-1; i < j; i, j = i+1, j-1 {
		chainB[i], chainB[j] = chainB[j], chainB[i]
	}

	return blocka, chainA, chainB
}
