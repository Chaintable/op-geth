package processor

import (
	"fmt"

	"github.com/ethereum/go-ethereum/pipeline/types"
	"github.com/ethereum/go-ethereum/pipeline/util"
)

type DataFile struct {
	S3key string
	Data  []byte
	Kind  string
}

// s3key: chain_id/block_id
// 外部s3
func SerializeFile(chainID string, blockFile *types.BlockFile) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(blockFile)
	if err != nil {
		return nil, err
	}
	s3Key := fmt.Sprintf("%s/%s", chainID, blockFile.Block.ID)
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_file",
	}, nil
}

// s3key: chain_id/block_height/block_id
// 外部s3,empty object,只用key
func SerializeFileValidation(chainID string, blockFile *types.BlockFile) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(blockFile.Validation())
	if err != nil {
		return nil, err
	}
	s3Key := fmt.Sprintf("%s/%d/%s", chainID, blockFile.Block.Height, blockFile.Block.ID)
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_file_validation",
	}, nil
}

// s3Key: <chainID>/<blockHash>/block
// 内部s3
func SerializeHeader(chainID string, header *types.Header) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(header)
	if err != nil {
		return nil, err
	}
	s3Key := fmt.Sprintf("%s/%s/block", chainID, header.Hash.String())
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_header",
	}, nil
}

// s3Key: <chainID>/<blockRoot>/stateDiff
// 内部s3
func SerializeStateDiff(chainID string, stateDiff *types.BlockStorageDiff) (*DataFile, error) {
	data, err := util.EncodeToRlp(stateDiff)
	if err != nil {
		return nil, err
	}
	s3Key := fmt.Sprintf("%s/%s/stateDiff", chainID, stateDiff.Hash.Hex())
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "state_diff",
	}, nil
}
