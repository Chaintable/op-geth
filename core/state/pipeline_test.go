package state

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// Live commits and RPC replays must export the same hashed, RLP-encoded state
// changes even though the execution client's commit payload is now typed.
func TestPipelineCommitStateDiff(t *testing.T) {
	tdb := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	t.Cleanup(func() { tdb.Close() })
	db := NewDatabase(tdb, nil)
	s, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatal(err)
	}
	account := common.HexToAddress("0x01")
	deleted := common.HexToAddress("0x02")
	slot := common.HexToHash("0x01")
	clearedSlot := common.HexToHash("0x02")
	s.SetBalance(account, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	s.SetBalance(deleted, uint256.NewInt(20), tracing.BalanceChangeUnspecified)
	s.SetState(account, slot, common.HexToHash("0x03"))
	s.SetState(account, clearedSlot, common.HexToHash("0x04"))
	parent, err := s.Commit(1, true, false)
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(parent, db)
	if err != nil {
		t.Fatal(err)
	}
	code := []byte{0x60, 0x01, 0x00}
	s.SetNonce(account, 7, tracing.NonceChangeUnspecified)
	s.SetCode(account, code, tracing.CodeChangeUnspecified)
	s.SetState(account, slot, common.HexToHash("0x05"))
	s.SetState(account, clearedSlot, common.Hash{})
	s.SelfDestruct(deleted)
	wantRoot, wantDestructs, wantAccounts, wantStorages, wantCodes, err := s.StateDiff(true)
	if err != nil {
		t.Fatal(err)
	}
	accountHash := crypto.Keccak256Hash(account[:])
	if _, ok := wantStorages[accountHash][crypto.Keccak256Hash(clearedSlot[:])]; !ok {
		t.Fatal("RPC state diff omitted the cleared storage slot")
	}
	if !reflect.DeepEqual(wantCodes[crypto.Keccak256Hash(code)], code) {
		t.Fatal("RPC state diff omitted the new contract code")
	}
	calls := 0
	s.SetOnCommitLogger(func(origin, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) {
		calls++
		if origin != parent || root != wantRoot {
			t.Errorf("unexpected state roots: %s -> %s", origin, root)
		}
		if !reflect.DeepEqual(destructs, wantDestructs) || !reflect.DeepEqual(accounts, wantAccounts) || !reflect.DeepEqual(storages, wantStorages) || !reflect.DeepEqual(codes, wantCodes) {
			t.Error("live commit state diff differs from RPC replay")
		}
		if _, err := db.Reader(root); err != nil {
			t.Errorf("commit hook ran before the new state was available: %v", err)
		}
	})
	root, err := s.Commit(2, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || root != wantRoot {
		t.Fatalf("commit hook calls = %d, root = %s", calls, root)
	}

	// Empty blocks are handled by the Writer's canonical-head notification path.
	s, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	s.SetOnCommitLogger(func(common.Hash, common.Hash, map[common.Hash]struct{}, map[common.Hash][]byte, map[common.Address][]byte, map[common.Hash]map[common.Hash][]byte, map[common.Address]map[common.Hash][]byte, map[common.Hash][]byte) {
		t.Error("empty state commit must not emit a second Pipeline notification")
	})
	if _, err := s.Commit(3, true, false); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineRawDumpResume(t *testing.T) {
	tdb := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	t.Cleanup(func() { tdb.Close() })
	db := NewDatabase(tdb, nil)
	s, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatal(err)
	}
	s.SetBalance(common.HexToAddress("0x01"), uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	root, err := s.Commit(1, true, false)
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	opts := &DumpConfig{SkipCode: true, SkipStorage: true}
	dump, err := s.RawDump2(opts, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dump.Accounts) != 1 {
		t.Fatalf("dumped %d accounts, want 1", len(dump.Accounts))
	}
	resumed, err := s.RawDump2(opts, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dump, resumed) {
		t.Fatal("resumed Bedrock dump differs from the initial dump")
	}
}

type pipelineFailingIteratee struct{ Database }

var errPipelineDump = errors.New("state iteration failed")

func (db pipelineFailingIteratee) Iteratee(common.Hash) (Iteratee, error) {
	return nil, errPipelineDump
}

func TestPipelineRawDumpError(t *testing.T) {
	tdb := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	t.Cleanup(func() { tdb.Close() })
	s, err := New(types.EmptyRootHash, pipelineFailingIteratee{NewDatabase(tdb, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RawDump2(&DumpConfig{}, t.TempDir()); !errors.Is(err, errPipelineDump) {
		t.Fatalf("dump error = %v, want %v", err, errPipelineDump)
	}
}
