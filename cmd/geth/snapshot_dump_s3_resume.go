// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

const (
	snapshotDumpS3ResumeCheckpointVersion = 1
	snapshotDumpS3CheckpointInterval      = 1000

	snapshotDumpS3CheckpointFile = "checkpoint.json"
	snapshotDumpS3AccountsFile   = "accounts.rlp.elems"
	snapshotDumpS3StoragesFile   = "storages.rlp.elems"
	snapshotDumpS3CodesFile      = "codes.rlp.elems"
	snapshotDumpS3CodeHashesFile = "code-hashes.bin"
	snapshotDumpS3StorageTemp    = "storage-values.tmp"
	snapshotDumpS3FinalFile      = "stateDiff.rlp"
)

type snapshotDumpS3SegmentCheckpoint struct {
	Count       uint64 `json:"count"`
	ContentSize uint64 `json:"content_size"`
}

type snapshotDumpS3Checkpoint struct {
	Version         int                             `json:"version"`
	ChainID         string                          `json:"chain_id"`
	PipelineVersion string                          `json:"pipeline_version"`
	BlockHash       common.Hash                     `json:"block_hash"`
	BlockNumber     uint64                          `json:"block_number"`
	Root            common.Hash                     `json:"root"`
	NextAccount     common.Hash                     `json:"next_account"`
	TraversalDone   bool                            `json:"traversal_done"`
	Finalized       bool                            `json:"finalized"`
	Complete        bool                            `json:"complete"`
	AccountCount    uint64                          `json:"account_count"`
	CodeCount       uint64                          `json:"code_count"`
	StorageCount    uint64                          `json:"storage_count"`
	Accounts        snapshotDumpS3SegmentCheckpoint `json:"accounts"`
	Storages        snapshotDumpS3SegmentCheckpoint `json:"storages"`
	Codes           snapshotDumpS3SegmentCheckpoint `json:"codes"`
	CodeHashesSize  uint64                          `json:"code_hashes_size"`
	UpdatedAt       string                          `json:"updated_at"`
}

type snapshotDumpS3Segment struct {
	path        string
	file        *os.File
	count       uint64
	contentSize uint64
}

type snapshotDumpS3Resumer struct {
	dir            string
	checkpointPath string
	cp             snapshotDumpS3Checkpoint

	accounts *snapshotDumpS3Segment
	storages *snapshotDumpS3Segment
	codes    *snapshotDumpS3Segment

	codeHashes     *os.File
	codeHashesSize uint64
	seenCodeHashes map[common.Hash]bool
}

func dumpStateDiffS3Resumable(chaindb ethdb.Database, triedb *triedb.Database, block *ethTypes.Block, publisher *snapshotDumpPublisher, chainID, version, dir string) error {
	resumer, err := newSnapshotDumpS3Resumer(dir, chainID, version, block)
	if err != nil {
		return err
	}
	defer resumer.close()

	if resumer.cp.Complete {
		log.Info("Snapshot dump-s3 resume directory is already complete", "dir", dir, "block", block.Hash(), "root", block.Root())
		return nil
	}
	if !resumer.cp.TraversalDone {
		if err := resumer.dumpStateDiff(chaindb, triedb, block); err != nil {
			return err
		}
	}
	localFile, err := resumer.finalizeStateDiff()
	if err != nil {
		return err
	}
	s3Key := snapshotDumpS3StateDiffKey(chainID, version, block.Root())
	if err := publisher.uploadNodeXLocalFile(s3Key, "state_diff", localFile); err != nil {
		return fmt.Errorf("failed to upload block state diff: %w", err)
	}
	blockChanges := &ptypes.BlockChangeNotification{
		ChangeType: 1,
		NewBlocks: []ptypes.BlockContext{
			{
				Hash:        block.Hash(),
				ParentHash:  block.ParentHash(),
				BlockNumber: block.NumberU64(),
				Timestamp:   block.Time(),
			},
		},
	}
	if err := publisher.pushBlockChangeNotification(blockChanges); err != nil {
		return err
	}
	resumer.cp.Complete = true
	return resumer.saveCheckpoint()
}

func newSnapshotDumpS3Resumer(dir, chainID, version string, block *ethTypes.Block) (*snapshotDumpS3Resumer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create resume dir %s: %w", dir, err)
	}
	resumer := &snapshotDumpS3Resumer{
		dir:            dir,
		checkpointPath: filepath.Join(dir, snapshotDumpS3CheckpointFile),
	}
	if err := resumer.loadOrCreateCheckpoint(chainID, version, block); err != nil {
		return nil, err
	}
	if resumer.cp.Complete {
		return resumer, nil
	}
	var err error
	if resumer.accounts, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3AccountsFile, resumer.cp.Accounts); err != nil {
		return nil, err
	}
	if resumer.storages, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3StoragesFile, resumer.cp.Storages); err != nil {
		resumer.close()
		return nil, err
	}
	if resumer.codes, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3CodesFile, resumer.cp.Codes); err != nil {
		resumer.close()
		return nil, err
	}
	if resumer.codeHashes, resumer.seenCodeHashes, err = openSnapshotDumpS3CodeHashes(dir, resumer.cp.CodeHashesSize); err != nil {
		resumer.close()
		return nil, err
	}
	resumer.codeHashesSize = resumer.cp.CodeHashesSize
	if err := os.Remove(filepath.Join(dir, snapshotDumpS3StorageTemp)); err != nil && !errors.Is(err, os.ErrNotExist) {
		resumer.close()
		return nil, fmt.Errorf("failed to remove stale storage temp file: %w", err)
	}
	return resumer, nil
}

func (r *snapshotDumpS3Resumer) loadOrCreateCheckpoint(chainID, version string, block *ethTypes.Block) error {
	data, err := os.ReadFile(r.checkpointPath)
	if errors.Is(err, os.ErrNotExist) {
		r.cp = snapshotDumpS3Checkpoint{
			Version:         snapshotDumpS3ResumeCheckpointVersion,
			ChainID:         chainID,
			PipelineVersion: version,
			BlockHash:       block.Hash(),
			BlockNumber:     block.NumberU64(),
			Root:            block.Root(),
		}
		return r.writeCheckpointFile()
	}
	if err != nil {
		return fmt.Errorf("failed to read snapshot dump-s3 checkpoint: %w", err)
	}
	if err := json.Unmarshal(data, &r.cp); err != nil {
		return fmt.Errorf("failed to decode snapshot dump-s3 checkpoint %s: %w", r.checkpointPath, err)
	}
	if r.cp.Version != snapshotDumpS3ResumeCheckpointVersion {
		return fmt.Errorf("unsupported snapshot dump-s3 checkpoint version %d", r.cp.Version)
	}
	if r.cp.ChainID != chainID || r.cp.PipelineVersion != version || r.cp.BlockHash != block.Hash() || r.cp.Root != block.Root() {
		return fmt.Errorf("snapshot dump-s3 resume dir %s belongs to chain=%s version=%s block=%s root=%s, not chain=%s version=%s block=%s root=%s",
			r.dir, r.cp.ChainID, r.cp.PipelineVersion, r.cp.BlockHash, r.cp.Root, chainID, version, block.Hash(), block.Root())
	}
	return nil
}

func openSnapshotDumpS3Segment(dir, name string, cp snapshotDumpS3SegmentCheckpoint) (*snapshotDumpS3Segment, error) {
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}
	if err := file.Truncate(int64(cp.ContentSize)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to truncate %s: %w", path, err)
	}
	if _, err := file.Seek(int64(cp.ContentSize), io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek %s: %w", path, err)
	}
	return &snapshotDumpS3Segment{
		path:        path,
		file:        file,
		count:       cp.Count,
		contentSize: cp.ContentSize,
	}, nil
}

func openSnapshotDumpS3CodeHashes(dir string, size uint64) (*os.File, map[common.Hash]bool, error) {
	if size%common.HashLength != 0 {
		return nil, nil, fmt.Errorf("invalid code hash index size %d", size)
	}
	path := filepath.Join(dir, snapshotDumpS3CodeHashesFile)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open %s: %w", path, err)
	}
	if err := file.Truncate(int64(size)); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to truncate %s: %w", path, err)
	}
	seen := make(map[common.Hash]bool)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to seek %s: %w", path, err)
	}
	var buf [common.HashLength]byte
	for read := uint64(0); read < size; read += common.HashLength {
		if _, err := io.ReadFull(file, buf[:]); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to read %s: %w", path, err)
		}
		seen[common.BytesToHash(buf[:])] = true
	}
	if _, err := file.Seek(int64(size), io.SeekStart); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to seek %s: %w", path, err)
	}
	return file, seen, nil
}

func (r *snapshotDumpS3Resumer) dumpStateDiff(chaindb ethdb.Database, triedb *triedb.Database, block *ethTypes.Block) error {
	stateTrie, err := trie.NewStateTrie(trie.StateTrieID(block.Root()), triedb)
	if err != nil {
		return err
	}
	accNodeIt, err := stateTrie.NodeIterator(r.cp.NextAccount.Bytes())
	if err != nil {
		return err
	}
	accIt := trie.NewIterator(accNodeIt)

	var (
		logged          = time.Now()
		start           = time.Now()
		lastCheckpoint  = r.cp.AccountCount
		checkpointEvery = uint64(snapshotDumpS3CheckpointInterval)
	)
	log.Info("Resumable snapshot dump-s3 stateDiff started", "dir", r.dir, "root", block.Root(), "nextAccount", r.cp.NextAccount,
		"accounts", r.cp.AccountCount, "codes", r.cp.CodeCount, "storages", r.cp.StorageCount)

	for accIt.Next() {
		if err := r.dumpAccount(chaindb, triedb, block.Root(), accIt.Key, accIt.Value); err != nil {
			return err
		}
		if r.cp.AccountCount-lastCheckpoint >= checkpointEvery {
			if err := r.saveCheckpoint(); err != nil {
				return err
			}
			lastCheckpoint = r.cp.AccountCount
		}
		if time.Since(logged) > 8*time.Second {
			log.Info("Snapshot dumping in progress", "at", r.cp.NextAccount, "accounts", r.cp.AccountCount, "codes", r.cp.CodeCount, "storages", r.cp.StorageCount,
				"elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	if accIt.Err != nil {
		log.Error("Failed to traverse state trie", "root", block.Root(), "err", accIt.Err)
		return accIt.Err
	}
	r.cp.TraversalDone = true
	r.cp.NextAccount = common.Hash{}
	if err := r.saveCheckpoint(); err != nil {
		return err
	}
	log.Info("Resumable snapshot dump-s3 stateDiff traversal complete", "dir", r.dir, "accounts", r.cp.AccountCount, "codes", r.cp.CodeCount, "storages", r.cp.StorageCount,
		"elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

func (r *snapshotDumpS3Resumer) dumpAccount(chaindb ethdb.Database, triedb *triedb.Database, root common.Hash, accountKey []byte, accountBlob []byte) error {
	var account ethTypes.StateAccount
	if err := rlp.DecodeBytes(accountBlob, &account); err != nil {
		log.Error("Invalid account encountered during state dump", "err", err)
		return err
	}
	if len(accountKey) != common.HashLength {
		return fmt.Errorf("invalid account trie key length: got %d, want %d", len(accountKey), common.HashLength)
	}
	accountHash := common.BytesToHash(accountKey)
	newAccount := ptypes.NewAccount{
		Address:  accountHash,
		Balance:  account.Balance,
		Nonce:    account.Nonce,
		CodeHash: common.BytesToHash(account.CodeHash),
	}
	if !bytes.Equal(account.CodeHash, ethTypes.EmptyCodeHash.Bytes()) && !r.seenCodeHashes[newAccount.CodeHash] {
		code := rawdb.ReadCode(chaindb, newAccount.CodeHash)
		if err := r.appendCode(ptypes.NewCode{CodeHash: newAccount.CodeHash, Code: code}); err != nil {
			return err
		}
	}
	storageSlots, err := r.appendAccountStorage(triedb, root, accountHash, account.Root)
	if err != nil {
		return err
	}
	if err := r.accounts.appendRLP(newAccount); err != nil {
		return err
	}
	r.cp.AccountCount++
	r.cp.StorageCount += storageSlots
	next, ok := nextSnapshotDumpS3Account(accountHash)
	if ok {
		r.cp.NextAccount = next
	} else {
		r.cp.TraversalDone = true
		r.cp.NextAccount = common.Hash{}
	}
	return nil
}

func (r *snapshotDumpS3Resumer) appendCode(code ptypes.NewCode) error {
	if err := r.codes.appendRLP(code); err != nil {
		return err
	}
	if _, err := r.codeHashes.Write(code.CodeHash.Bytes()); err != nil {
		return fmt.Errorf("failed to append code hash index: %w", err)
	}
	r.codeHashesSize += common.HashLength
	r.seenCodeHashes[code.CodeHash] = true
	r.cp.CodeCount++
	return nil
}

func (r *snapshotDumpS3Resumer) appendAccountStorage(triedb *triedb.Database, root, accountHash, storageRoot common.Hash) (uint64, error) {
	valuesPath := filepath.Join(r.dir, snapshotDumpS3StorageTemp)
	valuesFile, err := os.OpenFile(valuesPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open storage temp file: %w", err)
	}
	defer valuesFile.Close()

	var valuesContentSize, valuesCount uint64
	if storageRoot != ethTypes.EmptyRootHash {
		storageTrie, err := trie.NewStateTrie(trie.StorageTrieID(root, accountHash, storageRoot), triedb)
		if err != nil {
			log.Error("Failed to open storage trie", "account", accountHash, "root", storageRoot, "err", err)
			return 0, err
		}
		storageNodeIt, err := storageTrie.NodeIterator(nil)
		if err != nil {
			log.Error("Failed to open storage iterator", "account", accountHash, "root", storageRoot, "err", err)
			return 0, err
		}
		storageIt := trie.NewIterator(storageNodeIt)
		for storageIt.Next() {
			value := uint256.NewInt(0)
			if len(storageIt.Value) > 0 {
				_, content, _, err := rlp.Split(storageIt.Value)
				if err != nil {
					log.Error("Failed to split storage", "err", err)
					return 0, err
				}
				valueHash := common.BytesToHash(content)
				value = value.SetBytes(valueHash.Bytes())
			}
			if len(storageIt.Key) != common.HashLength {
				return 0, fmt.Errorf("invalid storage trie key length: account %s got %d, want %d", accountHash, len(storageIt.Key), common.HashLength)
			}
			size, err := encodeRLPElement(valuesFile, ptypes.IndexValuePair{
				Index: common.BytesToHash(storageIt.Key),
				Value: value,
			})
			if err != nil {
				return 0, err
			}
			valuesContentSize += size
			valuesCount++
		}
		if storageIt.Err != nil {
			log.Error("Failed to traverse storage trie", "account", accountHash, "root", storageRoot, "err", storageIt.Err)
			return 0, storageIt.Err
		}
	}
	size, err := writeSnapshotDumpS3AccountStorageDiff(r.storages.file, accountHash, valuesFile, valuesContentSize)
	if err != nil {
		return 0, err
	}
	r.storages.contentSize += size
	r.storages.count++
	return valuesCount, nil
}

func (r *snapshotDumpS3Resumer) finalizeStateDiff() (string, error) {
	finalPath := filepath.Join(r.dir, snapshotDumpS3FinalFile)
	if r.cp.Finalized {
		if _, err := os.Stat(finalPath); err == nil {
			return finalPath, nil
		}
		if err := os.Remove(finalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("failed to remove missing finalized stateDiff marker: %w", err)
		}
		r.cp.Finalized = false
	}
	if !r.cp.TraversalDone {
		return "", errors.New("cannot finalize snapshot dump-s3 stateDiff before traversal is complete")
	}
	if err := r.syncSegments(); err != nil {
		return "", err
	}
	tmpPath := finalPath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to create final stateDiff file: %w", err)
	}
	if err := r.writeFinalStateDiff(out); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return "", fmt.Errorf("failed to sync final stateDiff file: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("failed to close final stateDiff file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", fmt.Errorf("failed to install final stateDiff file: %w", err)
	}
	r.cp.Finalized = true
	if err := r.saveCheckpoint(); err != nil {
		return "", err
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		return "", err
	}
	log.Info("Finalized resumable snapshot dump-s3 stateDiff", "file", finalPath, "size", common.StorageSize(info.Size()),
		"accounts", r.cp.AccountCount, "codes", r.cp.CodeCount, "storages", r.cp.StorageCount)
	return finalPath, nil
}

func (r *snapshotDumpS3Resumer) writeFinalStateDiff(out io.Writer) error {
	hashRLP, err := rlp.EncodeToBytes(r.cp.Root)
	if err != nil {
		return err
	}
	parentHashRLP, err := rlp.EncodeToBytes(ethTypes.EmptyRootHash)
	if err != nil {
		return err
	}
	deletedAccountsRLP, err := rlp.EncodeToBytes([]common.Hash{})
	if err != nil {
		return err
	}
	accountsSize := rlp.ListSize(r.accounts.contentSize)
	storagesSize := rlp.ListSize(r.storages.contentSize)
	codesSize := rlp.ListSize(r.codes.contentSize)
	totalContentSize := uint64(len(hashRLP)+len(parentHashRLP)+len(deletedAccountsRLP)) + accountsSize + storagesSize + codesSize
	if err := writeRLPListHeader(out, totalContentSize); err != nil {
		return err
	}
	if _, err := out.Write(hashRLP); err != nil {
		return err
	}
	if _, err := out.Write(parentHashRLP); err != nil {
		return err
	}
	if err := writeSnapshotDumpS3SegmentList(out, r.accounts); err != nil {
		return err
	}
	if _, err := out.Write(deletedAccountsRLP); err != nil {
		return err
	}
	if err := writeSnapshotDumpS3SegmentList(out, r.storages); err != nil {
		return err
	}
	return writeSnapshotDumpS3SegmentList(out, r.codes)
}

func writeSnapshotDumpS3SegmentList(out io.Writer, segment *snapshotDumpS3Segment) error {
	if err := writeRLPListHeader(out, segment.contentSize); err != nil {
		return err
	}
	if _, err := segment.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek segment %s: %w", segment.path, err)
	}
	if _, err := io.Copy(out, io.LimitReader(segment.file, int64(segment.contentSize))); err != nil {
		return fmt.Errorf("failed to copy segment %s: %w", segment.path, err)
	}
	if _, err := segment.file.Seek(int64(segment.contentSize), io.SeekStart); err != nil {
		return fmt.Errorf("failed to restore segment %s: %w", segment.path, err)
	}
	return nil
}

func writeSnapshotDumpS3AccountStorageDiff(out io.Writer, accountHash common.Hash, values *os.File, valuesContentSize uint64) (uint64, error) {
	addressRLP, err := rlp.EncodeToBytes(accountHash)
	if err != nil {
		return 0, err
	}
	contentSize := uint64(len(addressRLP)) + rlp.ListSize(valuesContentSize)
	counter := &countingWriter{w: out}
	if err := writeRLPListHeader(counter, contentSize); err != nil {
		return 0, err
	}
	if _, err := counter.Write(addressRLP); err != nil {
		return 0, err
	}
	if err := writeRLPListHeader(counter, valuesContentSize); err != nil {
		return 0, err
	}
	if _, err := values.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek storage temp file: %w", err)
	}
	if _, err := io.Copy(counter, io.LimitReader(values, int64(valuesContentSize))); err != nil {
		return 0, fmt.Errorf("failed to copy storage temp file: %w", err)
	}
	return counter.n, nil
}

func (s *snapshotDumpS3Segment) appendRLP(value any) error {
	size, err := encodeRLPElement(s.file, value)
	if err != nil {
		return err
	}
	s.contentSize += size
	s.count++
	return nil
}

func encodeRLPElement(out io.Writer, value any) (uint64, error) {
	counter := &countingWriter{w: out}
	if err := rlp.Encode(counter, value); err != nil {
		return 0, err
	}
	return counter.n, nil
}

type countingWriter struct {
	w io.Writer
	n uint64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += uint64(n)
	return n, err
}

func (r *snapshotDumpS3Resumer) syncSegments() error {
	for _, segment := range []*snapshotDumpS3Segment{r.accounts, r.storages, r.codes} {
		if segment == nil || segment.file == nil {
			continue
		}
		if err := segment.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync %s: %w", segment.path, err)
		}
	}
	if r.codeHashes != nil {
		if err := r.codeHashes.Sync(); err != nil {
			return fmt.Errorf("failed to sync code hash index: %w", err)
		}
	}
	return nil
}

func (r *snapshotDumpS3Resumer) saveCheckpoint() error {
	if err := r.syncSegments(); err != nil {
		return err
	}
	if r.accounts != nil {
		r.cp.Accounts = snapshotDumpS3SegmentCheckpoint{Count: r.accounts.count, ContentSize: r.accounts.contentSize}
	}
	if r.storages != nil {
		r.cp.Storages = snapshotDumpS3SegmentCheckpoint{Count: r.storages.count, ContentSize: r.storages.contentSize}
	}
	if r.codes != nil {
		r.cp.Codes = snapshotDumpS3SegmentCheckpoint{Count: r.codes.count, ContentSize: r.codes.contentSize}
	}
	r.cp.CodeHashesSize = r.codeHashesSize
	return r.writeCheckpointFile()
}

func (r *snapshotDumpS3Resumer) writeCheckpointFile() error {
	r.cp.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(&r.cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.checkpointPath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint %s: %w", tmp, err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("failed to write checkpoint %s: %w", tmp, err)
	}
	if _, err := file.Write([]byte("\n")); err != nil {
		file.Close()
		return fmt.Errorf("failed to write checkpoint %s: %w", tmp, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync checkpoint %s: %w", tmp, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close checkpoint %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, r.checkpointPath); err != nil {
		return fmt.Errorf("failed to install checkpoint %s: %w", r.checkpointPath, err)
	}
	return nil
}

func (r *snapshotDumpS3Resumer) close() {
	for _, segment := range []*snapshotDumpS3Segment{r.accounts, r.storages, r.codes} {
		if segment != nil && segment.file != nil {
			segment.file.Close()
		}
	}
	if r.codeHashes != nil {
		r.codeHashes.Close()
	}
}

func writeRLPListHeader(out io.Writer, contentSize uint64) error {
	if contentSize <= 55 {
		_, err := out.Write([]byte{byte(0xc0 + contentSize)})
		return err
	}
	var buf [9]byte
	offset := len(buf)
	for size := contentSize; size > 0; size >>= 8 {
		offset--
		buf[offset] = byte(size)
	}
	sizeLen := len(buf) - offset
	buf[offset-1] = byte(0xf7 + sizeLen)
	_, err := out.Write(buf[offset-1:])
	return err
}

func nextSnapshotDumpS3Account(hash common.Hash) (common.Hash, bool) {
	next := hash
	for i := len(next) - 1; i >= 0; i-- {
		if next[i] != 0xff {
			next[i]++
			for j := i + 1; j < len(next); j++ {
				next[j] = 0
			}
			return next, true
		}
	}
	return common.Hash{}, false
}

func snapshotDumpS3StateDiffKey(chainID, version string, root common.Hash) string {
	if version == "" {
		return fmt.Sprintf("%s/%s/stateDiff", chainID, root.Hex())
	}
	return fmt.Sprintf("%s/%s/%s/stateDiff", chainID, version, root.Hex())
}
