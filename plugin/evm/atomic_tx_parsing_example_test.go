package evm

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestXxx(t *testing.T) {
	// Get these bytes from the block
	extData := common.Hex2Bytes("00000000000100000001000000010427d4b22a2a78bcddd456742caf91b56badbff985ee19aef14573e7343fd652000000000000000000000000000000000000000000000000000000000000000000000001f094dea89a47775fda83d32b558105b5de0ab2d400000011766dcb1921e67317cbc4be2aeb00677ad6462778a8f52274b9d605df2591b23027a87dff0000000000000d9e0000000121e67317cbc4be2aeb00677ad6462778a8f52274b9d605df2591b23027a87dff000000070000001176687040000000000000000000000001000000016f87598410356758ea4adf3378f495c37d624c75000000010000000900000001e8e7e78c254e5d110f558152a1e809d46c44e180cf0ac73aa947a89919219b2534ec78d946061aea50039936e8c64a0332bf15ded0882e21ee89ad1bc06977a201")

	isAp5 := true // Set based on the block's timestamp
	txs, err := ExtractAtomicTxs(extData, isAp5, Codec)
	require.NoError(t, err)

	for _, tx := range txs {
		// These are the bytes that are hashed to get the tx ID
		txBytes := tx.SignedBytes()
		t.Logf("%d: %s", len(txBytes), common.Bytes2Hex(txBytes))

		switch tx.UnsignedAtomicTx.(type) {
		case *UnsignedImportTx:
			t.Log("import")
		case *UnsignedExportTx:
			t.Log("export")
		}
	}
}
