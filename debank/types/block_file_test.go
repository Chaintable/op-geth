package types

import "testing"

func TestBlockFileTest(t *testing.T) {
	bf := BlockFile{
		Block: Block{
			ID: "abcd",
		},
		Txs: []Transaction{
			{ID: "efgh"},
		},
	}

	validationHash := bf.Validation()

	if validationHash.ValidationHash != 217265 {
		t.Errorf("Expected 54217265 but got %d", validationHash.ValidationHash)
	}
}
