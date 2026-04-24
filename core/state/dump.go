// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// DumpConfig is a set of options to control what portions of the state will be
// iterated and collected.
type DumpConfig struct {
	SkipCode          bool
	SkipStorage       bool
	OnlyWithAddresses bool
	Start             []byte
	Max               uint64
}

// DumpCollector interface which the state trie calls during iteration
type DumpCollector interface {
	// OnRoot is called with the state root
	OnRoot(common.Hash)
	// OnAccount is called once for each account in the trie
	OnAccount(*common.Address, DumpAccount)
}

// DumpAccount represents an account in the state.
type DumpAccount struct {
	Balance     string                 `json:"balance"`
	Nonce       uint64                 `json:"nonce"`
	Root        hexutil.Bytes          `json:"root"`
	CodeHash    hexutil.Bytes          `json:"codeHash"`
	Code        hexutil.Bytes          `json:"code,omitempty"`
	Storage     map[common.Hash]string `json:"storage,omitempty"`
	Address     *common.Address        `json:"address,omitempty"` // Address only present in iterative (line-by-line) mode
	AddressHash hexutil.Bytes          `json:"key,omitempty"`     // If we don't have address, we can output the key
}

// Dump represents the full dump in a collected format, as one large map.
type Dump struct {
	Root     string                 `json:"root"`
	Accounts map[string]DumpAccount `json:"accounts"`
	// Next can be set to represent that this dump is only partial, and Next
	// is where an iterator should be positioned in order to continue the dump.
	Next []byte `json:"next,omitempty"` // nil if no more accounts
}

type ProgressDump struct {
	ExportedCount uint64 `json:"exported_count"`
	NextKey       []byte `json:"next_key,omitempty"`
	Root          string `json:"root"`
	IsComplete    bool   `json:"is_complete"`
	BatchCount    int    `json:"batch_count"`
}

// OnRoot implements DumpCollector interface
func (d *Dump) OnRoot(root common.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

// OnAccount implements DumpCollector interface
func (d *Dump) OnAccount(addr *common.Address, account DumpAccount) {
	if addr == nil {
		d.Accounts[fmt.Sprintf("pre(%s)", account.AddressHash)] = account
	}
	if addr != nil {
		d.Accounts[(*addr).String()] = account
	}
}

// iterativeDump is a DumpCollector-implementation which dumps output line-by-line iteratively.
type iterativeDump struct {
	*json.Encoder
}

// OnAccount implements DumpCollector interface
func (d iterativeDump) OnAccount(addr *common.Address, account DumpAccount) {
	dumpAccount := &DumpAccount{
		Balance:     account.Balance,
		Nonce:       account.Nonce,
		Root:        account.Root,
		CodeHash:    account.CodeHash,
		Code:        account.Code,
		Storage:     account.Storage,
		AddressHash: account.AddressHash,
		Address:     addr,
	}
	d.Encode(dumpAccount)
}

// OnRoot implements DumpCollector interface
func (d iterativeDump) OnRoot(root common.Hash) {
	d.Encode(struct {
		Root common.Hash `json:"root"`
	}{root})
}

// DumpToCollector iterates the state according to the given options and inserts
// the items into a collector for aggregation or serialization.
//
// The state iterator is still trie-based and can be converted to snapshot-based
// once the state snapshot is fully integrated into database. TODO(rjl493456442).
func (s *StateDB) DumpToCollector(c DumpCollector, conf *DumpConfig) (nextKey []byte) {
	// Sanitize the input to allow nil configs
	if conf == nil {
		conf = new(DumpConfig)
	}
	var (
		missingPreimages int
		accounts         uint64
		start            = time.Now()
		logged           = time.Now()
	)
	log.Info("Trie dumping started", "root", s.originalRoot)
	c.OnRoot(s.originalRoot)

	tr, err := s.db.OpenTrie(s.originalRoot)
	if err != nil {
		return nil
	}
	trieIt, err := tr.NodeIterator(conf.Start)
	if err != nil {
		log.Error("Trie dumping error", "err", err)
		return nil
	}
	it := trie.NewIterator(trieIt)

	for it.Next() {
		var data types.StateAccount
		if err := rlp.DecodeBytes(it.Value, &data); err != nil {
			panic(err)
		}
		var (
			account = DumpAccount{
				Balance:     data.Balance.String(),
				Nonce:       data.Nonce,
				Root:        data.Root[:],
				CodeHash:    data.CodeHash,
				AddressHash: it.Key,
			}
			address   *common.Address
			addr      common.Address
			addrBytes = tr.GetKey(it.Key)
		)
		if addrBytes == nil {
			missingPreimages++
			if conf.OnlyWithAddresses {
				continue
			}
		} else {
			addr = common.BytesToAddress(addrBytes)
			address = &addr
			account.Address = address
		}
		obj := newObject(s, addr, &data)
		if !conf.SkipCode {
			account.Code = obj.Code()
		}
		if !conf.SkipStorage {
			account.Storage = make(map[common.Hash]string)

			storageTr, err := s.db.OpenStorageTrie(s.originalRoot, addr, obj.Root(), tr)
			if err != nil {
				log.Error("Failed to load storage trie", "err", err)
				continue
			}
			trieIt, err := storageTr.NodeIterator(nil)
			if err != nil {
				log.Error("Failed to create trie iterator", "err", err)
				continue
			}
			storageIt := trie.NewIterator(trieIt)
			for storageIt.Next() {
				_, content, _, err := rlp.Split(storageIt.Value)
				if err != nil {
					log.Error("Failed to decode the value returned by iterator", "error", err)
					continue
				}
				key := storageTr.GetKey(storageIt.Key)
				if key == nil {
					continue
				}
				account.Storage[common.BytesToHash(key)] = common.Bytes2Hex(content)
			}
		}
		c.OnAccount(address, account)
		accounts++
		if time.Since(logged) > 8*time.Second {
			log.Info("Trie dumping in progress", "at", common.Bytes2Hex(it.Key), "accounts", accounts,
				"elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
		if conf.Max > 0 && accounts >= conf.Max {
			if it.Next() {
				nextKey = it.Key
			}
			break
		}
	}
	if missingPreimages > 0 {
		log.Warn("Dump incomplete due to missing preimages", "missing", missingPreimages)
	}
	log.Info("Trie dumping complete", "accounts", accounts,
		"elapsed", common.PrettyDuration(time.Since(start)))

	return nextKey
}

// RawDump returns the state. If the processing is aborted e.g. due to options
// reaching Max, the `Next` key is set on the returned Dump.
func (s *StateDB) RawDump(opts *DumpConfig) Dump {
	dump := &Dump{
		Accounts: make(map[string]DumpAccount),
	}
	dump.Next = s.DumpToCollector(dump, opts)
	return *dump
}

// Dump returns a JSON string representing the entire state as a single json-object
func (s *StateDB) Dump(opts *DumpConfig) []byte {
	dump := s.RawDump(opts)
	json, err := json.MarshalIndent(dump, "", "    ")
	if err != nil {
		log.Error("Error dumping state", "err", err)
	}
	return json
}

// IterativeDump dumps out accounts as json-objects, delimited by linebreaks on stdout
func (s *StateDB) IterativeDump(opts *DumpConfig, output *json.Encoder) {
	s.DumpToCollector(iterativeDump{output}, opts)
}

func (s *StateDB) loadProgressFile(filePath string) (*ProgressDump, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var progress ProgressDump
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, err
	}
	return &progress, nil
}

func (s *StateDB) saveProgressFile(filePath string, progress *ProgressDump) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

func (s *StateDB) saveBatchFile(batchDir string, batchNum int, accounts map[string]DumpAccount) error {
	if err := os.MkdirAll(batchDir, 0755); err != nil {
		return err
	}
	batchFile := filepath.Join(batchDir, fmt.Sprintf("batch_%d.json", batchNum))
	data, err := json.Marshal(accounts)
	if err != nil {
		return err
	}
	return os.WriteFile(batchFile, data, 0644)
}

func (s *StateDB) loadAllBatches(batchDir string, batchCount int) (map[string]DumpAccount, error) {
	allAccounts := make(map[string]DumpAccount)
	for i := 1; i <= batchCount; i++ {
		batchFile := filepath.Join(batchDir, fmt.Sprintf("batch_%d.json", i))
		data, err := os.ReadFile(batchFile)
		if err != nil {
			return nil, err
		}
		var batchAccounts map[string]DumpAccount
		if err := json.Unmarshal(data, &batchAccounts); err != nil {
			return nil, err
		}
		for addr, acc := range batchAccounts {
			allAccounts[addr] = acc
		}
	}
	return allAccounts, nil
}

func (s *StateDB) dumpToCollector2(c DumpCollector, conf *DumpConfig) (nextKey []byte, err error) {
	if conf == nil {
		conf = new(DumpConfig)
	}
	var (
		missingPreimages        int
		missingStoragePreimages int
		storageTrieErrors       int
		accounts                uint64
		start                   = time.Now()
		logged                  = time.Now()
	)
	log.Info("[GenesisStateDump] Trie dumping started", "root", s.originalRoot)
	c.OnRoot(s.originalRoot)

	tr, err := s.db.OpenTrie(s.originalRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to open state trie at root %x: %w", s.originalRoot, err)
	}
	trieIt, err := tr.NodeIterator(conf.Start)
	if err != nil {
		return nil, fmt.Errorf("failed to create trie iterator at root %x: %w", s.originalRoot, err)
	}
	it := trie.NewIterator(trieIt)

	for it.Next() {
		var data types.StateAccount
		if err := rlp.DecodeBytes(it.Value, &data); err != nil {
			panic(err)
		}
		var (
			account = DumpAccount{
				Balance:     data.Balance.String(),
				Nonce:       data.Nonce,
				Root:        data.Root[:],
				CodeHash:    data.CodeHash,
				AddressHash: it.Key,
			}
			address   *common.Address
			addr      common.Address
			addrBytes = tr.GetKey(it.Key)
		)
		if addrBytes == nil {
			missingPreimages++
			if conf.OnlyWithAddresses {
				continue
			}
		} else {
			addr = common.BytesToAddress(addrBytes)
			address = &addr
			account.Address = address
		}
		obj := newObject(s, addr, &data)
		if !conf.SkipCode {
			account.Code = obj.Code()
		}
		if !conf.SkipStorage {
			account.Storage = make(map[common.Hash]string)

			storageTr, err := s.db.OpenStorageTrie(s.originalRoot, addr, obj.Root(), tr)
			if err != nil {
				storageTrieErrors++
				log.Error("[GenesisStateDump] Failed to load storage trie, account storage will be MISSING",
					"addr", addr, "storageRoot", obj.Root(), "err", err)
				continue
			}
			trieIt, err := storageTr.NodeIterator(nil)
			if err != nil {
				storageTrieErrors++
				log.Error("[GenesisStateDump] Failed to create storage trie iterator, account storage will be MISSING",
					"addr", addr, "storageRoot", obj.Root(), "err", err)
				continue
			}
			storageIt := trie.NewIterator(trieIt)
			for storageIt.Next() {
				_, content, _, err := rlp.Split(storageIt.Value)
				if err != nil {
					log.Error("[GenesisStateDump] Failed to decode the value returned by iterator", "error", err)
					continue
				}
				key := storageTr.GetKey(storageIt.Key)
				if key == nil {
					missingStoragePreimages++
					continue
				}
				account.Storage[common.BytesToHash(key)] = common.Bytes2Hex(content)
			}
		}
		c.OnAccount(address, account)
		accounts++
		if time.Since(logged) > 8*time.Second {
			log.Info("[GenesisStateDump] Trie dumping in progress", "at", common.Bytes2Hex(it.Key), "accounts", accounts,
				"elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
		if conf.Max > 0 && accounts >= conf.Max {
			if it.Next() {
				nextKey = it.Key
			}
			break
		}
	}
	if missingPreimages > 0 {
		log.Warn("[GenesisStateDump] Dump incomplete due to missing preimages", "missing", missingPreimages)
	}
	if missingStoragePreimages > 0 {
		log.Warn("[GenesisStateDump] Dump has missing storage key preimages, some storage slots were skipped",
			"missingStoragePreimages", missingStoragePreimages)
	}
	if storageTrieErrors > 0 {
		log.Error("[GenesisStateDump] Dump has storage trie errors, some accounts have MISSING storage",
			"storageTrieErrors", storageTrieErrors)
	}
	log.Info("[GenesisStateDump] Trie dumping complete", "accounts", accounts,
		"missingStoragePreimages", missingStoragePreimages,
		"storageTrieErrors", storageTrieErrors,
		"elapsed", common.PrettyDuration(time.Since(start)))

	return nextKey, nil
}

func (s *StateDB) RawDump2(opts *DumpConfig, dataDir string) (Dump, error) {
	const batchSize = 10000
	progressDir := filepath.Join(dataDir, "dump_bedrock_genesis")
	progressFile := filepath.Join(progressDir, "progress.json")
	batchDir := filepath.Join(progressDir, "batches")

	progress, err := s.loadProgressFile(progressFile)
	if err != nil {
		progress = &ProgressDump{}
	}

	if progress.IsComplete {
		allAccounts, err := s.loadAllBatches(batchDir, progress.BatchCount)
		if err != nil {
			log.Error("[GenesisStateDump] Failed to load batch files", "err", err)
			return Dump{}, fmt.Errorf("failed to load batch files: %w", err)
		}
		return Dump{
			Root:     progress.Root,
			Accounts: allAccounts,
		}, nil
	}

	if progress.Root == "" {
		progress.Root = fmt.Sprintf("%x", s.originalRoot)
	}

	batchOpts := &DumpConfig{
		SkipCode:          opts.SkipCode,
		SkipStorage:       opts.SkipStorage,
		OnlyWithAddresses: opts.OnlyWithAddresses,
		Start:             progress.NextKey,
		Max:               batchSize,
	}

	for {
		batchDump := &Dump{
			Accounts: make(map[string]DumpAccount),
		}

		nextKey, err := s.dumpToCollector2(batchDump, batchOpts)
		if err != nil {
			log.Error("[GenesisStateDump] dumpToCollector2 failed during RawDump2", "batch", progress.BatchCount+1, "err", err)
			return Dump{}, fmt.Errorf("dumpToCollector2 failed at batch %d: %w", progress.BatchCount+1, err)
		}

		if len(batchDump.Accounts) > 0 {
			progress.BatchCount++
			progress.ExportedCount += uint64(len(batchDump.Accounts))

			if err := s.saveBatchFile(batchDir, progress.BatchCount, batchDump.Accounts); err != nil {
				log.Error("[GenesisStateDump] Failed to save batch file", "batch", progress.BatchCount, "err", err)
				return Dump{}, fmt.Errorf("failed to save batch file %d: %w", progress.BatchCount, err)
			}
		}

		progress.NextKey = nextKey
		if nextKey == nil {
			if progress.ExportedCount > 0 {
				progress.IsComplete = true
			} else {
				log.Error("[GenesisStateDump] RawDump2 produced zero accounts, refusing to mark as complete",
					"root", progress.Root)
				return Dump{}, fmt.Errorf("dump produced zero accounts for root %s, state trie may be inaccessible", progress.Root)
			}
		}

		if err := s.saveProgressFile(progressFile, progress); err != nil {
			log.Error("[GenesisStateDump] Failed to save progress", "err", err)
		}

		log.Info("[GenesisStateDump] Batch completed", "batch", progress.BatchCount, "exported", progress.ExportedCount, "complete", progress.IsComplete)

		if progress.IsComplete {
			break
		}

		batchOpts.Start = nextKey
	}

	allAccounts, err := s.loadAllBatches(batchDir, progress.BatchCount)
	if err != nil {
		log.Error("[GenesisStateDump] Failed to load all batches", "err", err)
		return Dump{}, fmt.Errorf("failed to load all batches: %w", err)
	}

	return Dump{
		Root:     progress.Root,
		Accounts: allAccounts,
	}, nil
}
