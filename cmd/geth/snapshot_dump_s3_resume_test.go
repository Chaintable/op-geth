package main

import (
	"bytes"
	"os"
	"testing"

	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

func TestSnapshotDumpS3ResumableRLPMatchesBlockStorageDiff(t *testing.T) {
	dir := t.TempDir()
	root := common.HexToHash("0x1234")
	diff := &ptypes.BlockStorageDiff{
		Hash:       root,
		ParentHash: ethTypes.EmptyRootHash,
		NewAccounts: []ptypes.NewAccount{
			{
				Address:  common.HexToHash("0x01"),
				Balance:  uint256.NewInt(100),
				Nonce:    7,
				CodeHash: common.HexToHash("0xc0de"),
			},
			{
				Address:  common.HexToHash("0x02"),
				Balance:  uint256.NewInt(0),
				Nonce:    0,
				CodeHash: ethTypes.EmptyCodeHash,
			},
		},
		DeletedAccounts: []common.Hash{},
		StorageDiff: []ptypes.AccountStorageDiff{
			{
				Address: common.HexToHash("0x01"),
				Values: []ptypes.IndexValuePair{
					{Index: common.HexToHash("0xaa"), Value: uint256.NewInt(1)},
					{Index: common.HexToHash("0xbb"), Value: uint256.NewInt(2)},
				},
			},
			{
				Address: common.HexToHash("0x02"),
				Values:  []ptypes.IndexValuePair{},
			},
		},
		NewCodes: []ptypes.NewCode{
			{CodeHash: common.HexToHash("0xc0de"), Code: []byte{0x60, 0x00}},
		},
	}
	resumer := &snapshotDumpS3Resumer{
		dir: dir,
		cp: snapshotDumpS3Checkpoint{
			Root: diff.Hash,
		},
	}
	var err error
	if resumer.accounts, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3AccountsFile, snapshotDumpS3SegmentCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if resumer.storages, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3StoragesFile, snapshotDumpS3SegmentCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	if resumer.codes, err = openSnapshotDumpS3Segment(dir, snapshotDumpS3CodesFile, snapshotDumpS3SegmentCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	defer resumer.close()

	for _, account := range diff.NewAccounts {
		if err := resumer.accounts.appendRLP(account); err != nil {
			t.Fatal(err)
		}
	}
	for _, storage := range diff.StorageDiff {
		valuesFile, err := os.OpenFile(dir+"/values.tmp", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatal(err)
		}
		var valuesContentSize uint64
		for _, value := range storage.Values {
			size, err := encodeRLPElement(valuesFile, value)
			if err != nil {
				valuesFile.Close()
				t.Fatal(err)
			}
			valuesContentSize += size
		}
		size, err := writeSnapshotDumpS3AccountStorageDiff(resumer.storages.file, storage.Address, valuesFile, valuesContentSize)
		valuesFile.Close()
		if err != nil {
			t.Fatal(err)
		}
		resumer.storages.contentSize += size
		resumer.storages.count++
	}
	for _, code := range diff.NewCodes {
		if err := resumer.codes.appendRLP(code); err != nil {
			t.Fatal(err)
		}
	}

	var got bytes.Buffer
	if err := resumer.writeFinalStateDiff(&got); err != nil {
		t.Fatal(err)
	}
	want, err := rlp.EncodeToBytes(diff)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("streamed RLP mismatch\nhave %x\nwant %x", got.Bytes(), want)
	}
}

func TestNextSnapshotDumpS3Account(t *testing.T) {
	next, ok := nextSnapshotDumpS3Account(common.HexToHash("0x00ff"))
	if !ok {
		t.Fatal("expected next account")
	}
	if want := common.HexToHash("0x0100"); next != want {
		t.Fatalf("next mismatch: have %s want %s", next, want)
	}
	if _, ok := nextSnapshotDumpS3Account(common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")); ok {
		t.Fatal("expected overflow")
	}
}
