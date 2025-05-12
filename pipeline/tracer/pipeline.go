package tracer

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/pipeline/metrics"
	"github.com/ethereum/go-ethereum/pipeline/processor"
	ptypes "github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type ExtraInfo struct {
	BlockNumber uint64
	BlockHash   common.Hash
	BlockFile   *ptypes.BlockFile
	Tx          *types.Transaction
	From        common.Address
	BlockHeader *ptypes.Header
	BlockDiff   *ptypes.BlockStorageDiff
	BlockChange *ptypes.BlockChangeNotification
	Committed   bool
	// metrics timer
	TxStartTime    time.Time
	BlockStartTime time.Time
}

var (
	NodeXPusher            *processor.PushProcessor
	ChainTableBucketPusher *processor.PushProcessor
	BlockCtx               *ExtraInfo
	BizChainID             string
)

func InitPipeline(region string, nodeXBucket string, chainTableBucket string, brokers []string, topic string, bizChainID string, s3TmpDir string, isBackup bool) (err error) {
	NodeXPusher, err = processor.NewPushProcessor(region, nodeXBucket, brokers, topic, s3TmpDir, isBackup)
	if err != nil {
		return err
	}
	ChainTableBucketPusher, err = processor.NewPushProcessor(region, chainTableBucket, brokers, topic, s3TmpDir, isBackup)
	if err != nil {
		return err
	}
	BizChainID = bizChainID
	return nil
}

func stateUpdateToStateDiff(originRoot common.Hash, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, accountsOrigin map[common.Address][]byte, storages map[common.Hash]map[common.Hash][]byte, storagesOrigin map[common.Address]map[common.Hash][]byte, codes map[common.Hash][]byte) *ptypes.BlockStorageDiff {
	stateDiff := &ptypes.BlockStorageDiff{}
	for addrhash := range destructs {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addrhash)
	}
	for k, v := range accounts {
		account, _ := types.FullAccount(v)
		stateDiff.NewAccounts = append(stateDiff.NewAccounts, ptypes.NewAccount{
			Address:  k,
			Balance:  account.Balance,
			Nonce:    uint64(account.Nonce),
			CodeHash: common.BytesToHash(account.CodeHash),
		})
	}
	for account, storage := range storages {
		Values := make([]ptypes.IndexValuePair, 0, len(storage))
		for index, v := range storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				_, content, _, err := rlp.Split(v)
				if err != nil {
					log.Error("Failed to split storage", "err", err)
				}
				value = uint256.NewInt(0).SetBytes(content)
			}
			Values = append(Values, ptypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		stateDiff.StorageDiff = append(stateDiff.StorageDiff, ptypes.AccountStorageDiff{
			Address: account,
			Values:  Values,
		})
	}
	for hash, code := range codes {
		stateDiff.NewCodes = append(stateDiff.NewCodes, ptypes.NewCode{
			CodeHash: hash,
			Code:     code,
		})
	}
	if originRoot == (common.Hash{}) {
		originRoot = types.EmptyRootHash
	}
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	stateDiff.Hash = root
	stateDiff.ParentHash = originRoot
	return stateDiff
}

func GenesisAllocToStateDiff(genesisAlloc types.GenesisAlloc) *ptypes.BlockStorageDiff {
	diff := &ptypes.BlockStorageDiff{}
	diff.NewAccounts = make([]ptypes.NewAccount, 0)
	diff.NewCodes = make([]ptypes.NewCode, 0)
	diff.StorageDiff = make([]ptypes.AccountStorageDiff, 0)
	diff.DeletedAccounts = make([]common.Hash, 0)
	for addr, acc := range genesisAlloc {
		diff.NewAccounts = append(diff.NewAccounts, ptypes.NewAccount{
			Address:  crypto.Keccak256Hash(addr[:]),
			Balance:  uint256.MustFromBig(acc.Balance),
			Nonce:    acc.Nonce,
			CodeHash: crypto.Keccak256Hash(acc.Code),
		})
		if len(acc.Code) > 0 {
			diff.NewCodes = append(diff.NewCodes, ptypes.NewCode{
				CodeHash: crypto.Keccak256Hash(acc.Code),
				Code:     acc.Code,
			})
		}
		values := make([]ptypes.IndexValuePair, 0)
		for index, v := range acc.Storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				value = uint256.NewInt(0).SetBytes(v.Bytes())
			}
			values = append(values, ptypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		diff.StorageDiff = append(diff.StorageDiff, ptypes.AccountStorageDiff{
			Address: crypto.Keccak256Hash(addr[:]),
			Values:  values,
		})
	}
	return diff
}

func uploadBlockHeader(blockHeader *ptypes.Header) error {
	start := time.Now()
	defer func() {
		metrics.BlockHeaderUploadTimer.UpdateSince(start)
	}()
	s3BlockFile, err := processor.SerializeHeader(BizChainID, blockHeader)
	if err != nil {
		return fmt.Errorf("failed to serialize block header: %v", err)
	}
	err = NodeXPusher.UploadFile(s3BlockFile)
	if err != nil {
		return fmt.Errorf("failed to upload block header: %v", err)
	}
	return nil
}

func uploadBlockDiff(blockDiff *ptypes.BlockStorageDiff) error {
	start := time.Now()
	defer func() {
		metrics.StateDiffUploadTimer.UpdateSince(start)
	}()
	s3file, err := processor.SerializeStateDiff(BizChainID, blockDiff)
	if err != nil {
		return fmt.Errorf("failed to serialize state diff: %v", err)
	}
	err = NodeXPusher.UploadFile(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload state diff: %v", err)
	}
	return nil
}

func uploadBlockFile(blockFile *ptypes.BlockFile) error {
	s3file, err := processor.SerializeFile(BizChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file: %v", err)
	}
	err = ChainTableBucketPusher.UploadFile(s3file)
	if err != nil {
		return fmt.Errorf("failed to upload block file: %v", err)
	}
	return nil
}

func uploadblockFileValidation(blockFile *ptypes.BlockFile) error {
	start := time.Now()
	defer func() {
		metrics.BlockFileValidationTimer.UpdateSince(start)
	}()
	blockFileValidation, err := processor.SerializeFileValidation(BizChainID, blockFile)
	if err != nil {
		return fmt.Errorf("failed to serialize block file validation: %v", err)
	}
	err = ChainTableBucketPusher.UploadFile(blockFileValidation)
	if err != nil {
		return fmt.Errorf("failed to upload block file validation: %v", err)
	}
	return nil
}
