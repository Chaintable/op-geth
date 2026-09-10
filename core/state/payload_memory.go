package state

import "github.com/ethereum/go-ethereum/common"

// OriginalRoot identifies the state from which this block was executed.
func (s *StateDB) OriginalRoot() common.Hash { return s.originalRoot }

// EstimatedMemorySize estimates data retained by a completed payload. It includes
// read caches and allows for trie paths, maps and allocator overhead. Shared DB,
// snapshot and code caches are not owned by the payload. This is an admission
// budget, not an exact measurement or an RSS cap. Call only with exclusive access
// after IntermediateRoot has finished and the prefetcher has stopped.
func (s *StateDB) EstimatedMemorySize() uint64 {
	size := uint64(4096)
	for _, obj := range s.stateObjects {
		if obj == nil {
			continue
		}
		// Allow for account/storage trie paths as well as the cached values.
		size += uint64(8192 + len(obj.code) + len(obj.dirtyCodeHash))
		size += uint64(len(obj.originStorage)+len(obj.pendingStorage)+len(obj.dirtyStorage)+len(obj.commitStorage)) * 8192
	}
	for _, account := range s.accounts {
		size += uint64(128 + len(account))
	}
	for _, account := range s.accountsOrigin {
		size += uint64(128 + len(account))
	}
	for _, slots := range s.storages {
		size += 128
		for _, value := range slots {
			size += uint64(128 + len(value))
		}
	}
	for _, slots := range s.storagesOrigin {
		size += 128
		for _, value := range slots {
			size += uint64(128 + len(value))
		}
	}
	size += uint64(len(s.stateObjectsDestruct)+len(s.stateObjectsDestructDirty)+len(s.stateObjectsDirty)+len(s.stateObjectsPending)) * 256
	for _, logs := range s.logs {
		for _, entry := range logs {
			size += uint64(256 + len(entry.Data) + len(entry.Topics)*common.HashLength)
		}
	}
	for _, preimage := range s.preimages {
		size += uint64(128 + len(preimage))
	}
	return size
}
