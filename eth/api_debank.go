package eth

import (
	"context"
	"fmt"

	ptracer "github.com/Chaintable/pipeline/tracer"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
)

type DebankAPI struct {
	eth *Ethereum
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	return &DebankAPI{eth: eth}
}

func (api *DebankAPI) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ptypes.DebankOutPut, error) {
	block, err := api.eth.APIBackend.BlockByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found")
	}
	if block.NumberU64() == 0 {
		return nil, fmt.Errorf("genesis block trace not supported")
	}

	parent := api.eth.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent block not found")
	}
	statedb, err := api.eth.blockchain.StateAt(parent.Root())
	if err != nil {
		return nil, err
	}

	rpcTracer := ptracer.RPCTracer{}
	vmConfig := vm.Config{
		Tracer: &rpcTracer,
	}

	rpcTracer.OnBlockStart(block)

	chainConfig := api.eth.blockchain.Config()

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	var (
		txs     = block.Transactions()
		header  = block.Header()
		signer  = types.MakeSigner(chainConfig, header.Number, header.Time)
		gp      = new(core.GasPool).AddGas(block.GasLimit())
		usedGas = new(uint64)
	)

	blockContext := core.NewEVMBlockContext(header, api.eth.blockchain, nil, chainConfig, statedb)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, chainConfig, vmConfig)

	for i, tx := range txs {
		statedb.SetTxContext(tx.Hash(), i)

		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		rpcTracer.OnTxStart(tx, msg.From)

		txContext := core.NewEVMTxContext(msg)
		vmenv.Reset(txContext, statedb)

		result, err := core.ApplyMessage(vmenv, msg, gp)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		var root []byte
		if chainConfig.IsByzantium(header.Number) {
			statedb.Finalise(true)
		} else {
			root = statedb.IntermediateRoot(chainConfig.IsEIP158(header.Number)).Bytes()
		}
		*usedGas += result.UsedGas

		receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
		if result.Failed() {
			receipt.Status = types.ReceiptStatusFailed
		} else {
			receipt.Status = types.ReceiptStatusSuccessful
		}
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas

		if msg.To == nil {
			receipt.ContractAddress = crypto.CreateAddress(vmenv.TxContext.Origin, tx.Nonce())
		}

		receipt.Logs = statedb.GetLogs(tx.Hash(), header.Number.Uint64(), block.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockHash = block.Hash()
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = uint(statedb.TxIndex())

		rpcTracer.OnTxEnd(receipt, nil)
	}

	api.eth.engine.Finalize(api.eth.blockchain, header, statedb, block.Transactions(), block.Uncles(), block.Withdrawals())

	root, destructs, accounts, storages, codes, err := statedb.StateDiff(chainConfig.IsEIP158(block.Number()))
	if err != nil {
		return nil, fmt.Errorf("could not get state diff: %w", err)
	}

	if root != block.Header().Root {
		return nil, fmt.Errorf("state root mismatch: expected %x, got %x", block.Header().Root, root)
	}

	parentRoot := parent.Root()
	res := rpcTracer.GetOutPut(parentRoot, root, destructs, accounts, storages, codes)

	return res, nil
}
