package types

import (
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"strconv"
)

// BlockValidation represents validation information for a block
type BlockValidation struct {
	ValidationHash int64 `json:"validation_hash"`
	IsFork         bool  `json:"is_fork"`
}

// Validation calculates validation hash for a BlockFile
func Validation(bf *BlockFile) BlockValidation {
	var ids []string

	// Collect all IDs
	ids = append(ids, bf.Block.ID)

	for _, tx := range bf.Txs {
		ids = append(ids, tx.ID)
	}

	for _, event := range bf.Events {
		ids = append(ids, event.ID)
	}

	for _, trace := range bf.Traces {
		ids = append(ids, trace.ID)
	}

	// Calculate validation hash
	validationHash := CalcValidationHash(ids)

	return BlockValidation{
		ValidationHash: validationHash,
		IsFork:         false, // opBNB doesn't need to track forks in our use case
	}
}

// CalcValidationHash calculates a validation hash from a list of IDs
func CalcValidationHash(ids []string) int64 {
	sha1Sum := big.NewInt(0)

	for _, id := range ids {
		h := sha1.New()
		h.Write([]byte(id))
		hash := hex.EncodeToString(h.Sum(nil))

		hashInt := new(big.Int)
		hashInt.SetString(hash, 16)

		sha1Sum.Add(sha1Sum, hashInt)
	}

	// Take the last 6 digits as validation hash
	sha1SumStr := sha1Sum.String()
	last6Digits := sha1SumStr
	if len(sha1SumStr) > 6 {
		last6Digits = sha1SumStr[len(sha1SumStr)-6:]
	}

	validationHash, _ := strconv.ParseInt(last6Digits, 10, 64)
	return validationHash
}
