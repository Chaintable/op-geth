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

// s3key: chain_id/block_id (version为空时)
//        chain_id/version/block_id (version不为空时)
// 外部s3
func SerializeFile(chainID, version string, blockFile *types.BlockFile) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(blockFile)
	if err != nil {
		return nil, err
	}
	var s3Key string
	if version == "" {
		s3Key = fmt.Sprintf("%s/%s", chainID, blockFile.Block.ID)
	} else {
		s3Key = fmt.Sprintf("%s/%s/%s", chainID, version, blockFile.Block.ID)
	}
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_file",
	}, nil
}

// s3key: chain_id/block_height/block_id (version为空时)
//        chain_id/version/block_height/block_id (version不为空时)
// 外部s3,empty object,只用key
func SerializeFileValidation(chainID, version string, blockFile *types.BlockFile) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(blockFile.Validation())
	if err != nil {
		return nil, err
	}
	var s3Key string
	if version == "" {
		s3Key = fmt.Sprintf("%s/%d/%s", chainID, blockFile.Block.Height, blockFile.Block.ID)
	} else {
		s3Key = fmt.Sprintf("%s/%s/%d/%s", chainID, version, blockFile.Block.Height, blockFile.Block.ID)
	}
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_file_validation",
	}, nil
}

// s3Key: <chainID>/<blockHash>/block (version为空时)
//        <chainID>/<version>/<blockHash>/block (version不为空时)
// 内部s3
func SerializeHeader(chainID, version string, header *types.Header) (*DataFile, error) {
	data, err := util.EncodeToJsonGzip(header)
	if err != nil {
		return nil, err
	}
	var s3Key string
	if version == "" {
		s3Key = fmt.Sprintf("%s/%s/block", chainID, header.Hash.String())
	} else {
		s3Key = fmt.Sprintf("%s/%s/%s/block", chainID, version, header.Hash.String())
	}
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "block_header",
	}, nil
}

// s3Key: <chainID>/<blockRoot>/stateDiff (version为空时)
//        <chainID>/<version>/<blockRoot>/stateDiff (version不为空时)
// 内部s3
func SerializeStateDiff(chainID, version string, stateDiff *types.BlockStorageDiff) (*DataFile, error) {
	data, err := util.EncodeToRlp(stateDiff)
	if err != nil {
		return nil, err
	}
	var s3Key string
	if version == "" {
		s3Key = fmt.Sprintf("%s/%s/stateDiff", chainID, stateDiff.Hash.Hex())
	} else {
		s3Key = fmt.Sprintf("%s/%s/%s/stateDiff", chainID, version, stateDiff.Hash.Hex())
	}
	return &DataFile{
		S3key: s3Key,
		Data:  data,
		Kind:  "state_diff",
	}, nil
}
