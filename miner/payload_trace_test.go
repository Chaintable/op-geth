package miner

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"testing"

	ptracer "github.com/Chaintable/pipeline/tracer"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// This compares the retained build state with a fresh import execution, including
// a reverted storage write, a successful log and a withdrawal. It deliberately
// does not initialize the pipeline publishers or depend on any external RPC.
func TestTracedPayloadMatchesFreshImport(t *testing.T) {
	config := *params.AllDevChainProtocolChanges
	zero := uint64(0)
	config.Optimism = &params.OptimismConfig{EIP1559Elasticity: 50, EIP1559Denominator: 10}
	config.BedrockBlock = big.NewInt(0)
	config.RegolithTime = &zero
	config.CanyonTime = &zero
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	success := common.HexToAddress("0x1000")
	reverted := common.HexToAddress("0x2000")
	recipient := common.HexToAddress("0x3000")
	genesis := &core.Genesis{
		Config: &config, GasLimit: 30000000, Difficulty: big.NewInt(0), BaseFee: big.NewInt(params.InitialBaseFee),
		Alloc: types.GenesisAlloc{
			sender:   {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			success:  {Code: common.FromHex("0x602a60005560006000a000"), Balance: big.NewInt(0)},
			reverted: {Code: common.FromHex("0x602a60005560006000fd"), Balance: big.NewInt(0)},
		},
	}
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	engine := beacon.New(ethash.NewFaker())
	chain, err := core.NewBlockChain(db, &core.CacheConfig{TrieDirtyDisabled: true}, genesis, nil, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer chain.Stop()
	parent := chain.CurrentBlock()
	header := &types.Header{
		ParentHash: parent.Hash(), Number: big.NewInt(1), Time: 2,
		GasLimit: parent.GasLimit, Difficulty: big.NewInt(0),
		BaseFee: eip1559.CalcBaseFee(&config, parent, 2),
	}
	signer := types.MakeSigner(&config, header.Number, header.Time)
	var txs types.Transactions
	for i, address := range []common.Address{success, reverted} {
		tx, err := types.SignTx(types.NewTransaction(uint64(i), address, big.NewInt(0), 100000, big.NewInt(2*params.InitialBaseFee), nil), signer, key)
		if err != nil {
			t.Fatal(err)
		}
		txs = append(txs, tx)
	}
	builtState, err := chain.StateAt(parent.Root)
	if err != nil {
		t.Fatal(err)
	}
	referenceState, err := chain.StateAt(parent.Root)
	if err != nil {
		t.Fatal(err)
	}
	w := &worker{chain: chain, chainConfig: &config, engine: engine}
	args := &generateParams{noTxs: true, reuseExecution: true, txs: txs, withdrawals: types.Withdrawals{{Address: recipient, Amount: 7}}}
	result := w.generateTracedPayload(&environment{header: header, state: builtState}, args)
	if result.err != nil {
		t.Fatal(result.err)
	}
	cached := result.execution
	if cached == nil || !cached.Trace.MatchesPayload(result.block) {
		t.Fatal("build did not retain a complete trace")
	}
	vmConfig := vm.Config{Tracer: ptracer.NewPayloadTracer(result.block.Header())}
	receipts, logs, gas, err := core.NewStateProcessor(&config, chain, engine).Process(result.block, referenceState, vmConfig)
	if err != nil {
		t.Fatal(err)
	}
	if gas != cached.UsedGas || !reflect.DeepEqual(receipts, cached.Receipts) || !reflect.DeepEqual(logs, cached.Logs) {
		t.Fatal("retained gas, receipts or logs differ from a fresh import")
	}
	if len(logs) != 1 || receipts[1].Status != types.ReceiptStatusFailed {
		t.Fatal("fixture did not exercise logs and transaction reverts")
	}
	validator := core.NewBlockValidator(&config, chain, engine)
	if err := validator.ValidateState(result.block, cached.State, cached.Receipts, cached.UsedGas, false); err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateState(result.block, referenceState, receipts, gas, false); err != nil {
		t.Fatal(err)
	}
	wantWithdrawal := new(big.Int).Mul(big.NewInt(7), big.NewInt(params.GWei))
	if cached.State.GetBalance(recipient).ToBig().Cmp(wantWithdrawal) != 0 {
		t.Fatal("withdrawal was omitted or applied twice")
	}
	if cached.State.GetState(reverted, common.Hash{}) != (common.Hash{}) {
		t.Fatal("reverted storage write leaked into the retained state")
	}
	builtDiff := capturePayloadCommit(t, cached.State, result.block)
	referenceDiff := capturePayloadCommit(t, referenceState, result.block)
	if !bytes.Equal(builtDiff, referenceDiff) {
		t.Fatal("retained original values or final state diff differ from a fresh import")
	}
}

func capturePayloadCommit(t *testing.T, statedb *state.StateDB, block *types.Block) []byte {
	t.Helper()
	var captured []byte
	statedb.SetLiveTraceHooks(nil, func(parent, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, origins map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storageOrigins map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) {
		var err error
		captured, err = json.Marshal(struct {
			Parent, Root   common.Hash
			Destructs      map[common.Hash]struct{}
			Accounts       map[common.Hash][]byte
			Origins        map[common.Address][]byte
			Storages       map[common.Hash]map[common.Hash][]byte
			StorageOrigins map[common.Address]map[common.Hash][]byte
			Codes          map[common.Hash][]byte
		}{parent, root, destructs, accounts, origins, storages, storageOrigins, codes})
		if err != nil {
			t.Error(err)
		}
	})
	statedb.SetExpectedStateRoot(block.Root())
	root, err := statedb.Commit(block.NumberU64(), true)
	if err != nil || root != block.Root() || captured == nil {
		t.Fatalf("commit failed: root=%s err=%v", root, err)
	}
	return captured
}
