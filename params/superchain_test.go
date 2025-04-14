package params

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/superchain"
	"github.com/stretchr/testify/require"
)

type HumanProtocolVersion struct {
	VersionType         uint8
	Major, Minor, Patch uint32
	Prerelease          uint32
	Build               [8]byte
}

type ComparisonCase struct {
	A, B HumanProtocolVersion
	Cmp  ProtocolVersionComparison
}

func TestProtocolVersion_Compare(t *testing.T) {
	testCases := []ComparisonCase{
		{
			A:   HumanProtocolVersion{0, 2, 1, 1, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 2, 2, 2, [8]byte{}},
			Cmp: AheadMajor,
		},
		{
			A:   HumanProtocolVersion{0, 1, 2, 1, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 1, 2, 2, [8]byte{}},
			Cmp: AheadMinor,
		},
		{
			A:   HumanProtocolVersion{0, 1, 1, 2, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 1, 1, 2, [8]byte{}},
			Cmp: AheadPatch,
		},
		{
			A:   HumanProtocolVersion{0, 1, 1, 1, 2, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 1, 1, 1, [8]byte{}},
			Cmp: AheadPrerelease,
		},
		{
			A:   HumanProtocolVersion{0, 1, 2, 3, 4, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 2, 3, 4, [8]byte{}},
			Cmp: Matching,
		},
		{
			A:   HumanProtocolVersion{0, 3, 2, 1, 5, [8]byte{3}},
			B:   HumanProtocolVersion{1, 1, 2, 3, 3, [8]byte{6}},
			Cmp: DiffVersionType,
		},
		{
			A:   HumanProtocolVersion{0, 3, 2, 1, 5, [8]byte{3}},
			B:   HumanProtocolVersion{0, 1, 2, 3, 3, [8]byte{6}},
			Cmp: DiffBuild,
		},
		{
			A:   HumanProtocolVersion{0, 0, 0, 0, 0, [8]byte{}},
			B:   HumanProtocolVersion{0, 1, 3, 3, 3, [8]byte{3}},
			Cmp: EmptyVersion,
		},
		{
			A:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 1, [8]byte{}},
			Cmp: AheadMajor,
		},
		{
			A:   HumanProtocolVersion{0, 4, 1, 0, 0, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 1, 0, 1, [8]byte{}},
			Cmp: AheadMinor,
		},
		{
			A:   HumanProtocolVersion{0, 4, 0, 1, 0, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 1, 1, [8]byte{}},
			Cmp: AheadPatch,
		},
		{
			A:   HumanProtocolVersion{0, 4, 0, 0, 2, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 1, [8]byte{}},
			Cmp: AheadPrerelease,
		},
		{
			A:   HumanProtocolVersion{0, 4, 1, 0, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			Cmp: AheadPatch,
		},
		{
			A:   HumanProtocolVersion{0, 4, 0, 1, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			Cmp: AheadPrerelease,
		},
		{
			A:   HumanProtocolVersion{0, 4, 1, 1, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			Cmp: AheadMinor,
		},
		{
			A:   HumanProtocolVersion{0, 4, 0, 2, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			Cmp: AheadPatch,
		},
		{
			A:   HumanProtocolVersion{0, 5, 0, 1, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 4, 0, 0, 0, [8]byte{}},
			Cmp: AheadMajor,
		},
		{
			A:   HumanProtocolVersion{0, 1, 0, 0, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 0, 9, 0, 0, [8]byte{}},
			Cmp: AheadMinor,
		},
		{
			A:   HumanProtocolVersion{0, 0, 1, 0, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 0, 0, 9, 0, [8]byte{}},
			Cmp: AheadPatch,
		},
		{
			A:   HumanProtocolVersion{0, 1, ^uint32(0), 0, 1, [8]byte{}},
			B:   HumanProtocolVersion{0, 0, 1, 0, 0, [8]byte{}},
			Cmp: InvalidVersion,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			a := ProtocolVersionV0{tc.A.Build, tc.A.Major, tc.A.Minor, tc.A.Patch, tc.A.Prerelease}.Encode()
			a[0] = tc.A.VersionType
			b := ProtocolVersionV0{tc.B.Build, tc.B.Major, tc.B.Minor, tc.B.Patch, tc.B.Prerelease}.Encode()
			b[0] = tc.B.VersionType
			cmp := a.Compare(b)
			if cmp != tc.Cmp {
				t.Fatalf("expected %d but got %d", tc.Cmp, cmp)
			}
			switch tc.Cmp {
			case AheadMajor, AheadMinor, AheadPatch, AheadPrerelease:
				inv := b.Compare(a)
				if inv != -tc.Cmp {
					t.Fatalf("expected inverse when reversing the comparison, %d but got %d", -tc.Cmp, inv)
				}
			case DiffVersionType, DiffBuild, EmptyVersion, Matching:
				inv := b.Compare(a)
				if inv != tc.Cmp {
					t.Fatalf("expected comparison reversed to hold the same, expected %d but got %d", tc.Cmp, inv)
				}
			}
		})
	}
}
func TestProtocolVersion_String(t *testing.T) {
	testCases := []struct {
		version  ProtocolVersion
		expected string
	}{
		{ProtocolVersionV0{[8]byte{}, 0, 0, 0, 0}.Encode(), "v0.0.0"},
		{ProtocolVersionV0{[8]byte{}, 0, 0, 0, 1}.Encode(), "v0.0.0-1"},
		{ProtocolVersionV0{[8]byte{}, 0, 0, 1, 0}.Encode(), "v0.0.1"},
		{ProtocolVersionV0{[8]byte{}, 4, 3, 2, 1}.Encode(), "v4.3.2-1"},
		{ProtocolVersionV0{[8]byte{}, 0, 100, 2, 0}.Encode(), "v0.100.2"},
		{ProtocolVersionV0{[8]byte{'O', 'P', '-', 'm', 'o', 'd'}, 42, 0, 2, 1}.Encode(), "v42.0.2-1+OP-mod"},
		{ProtocolVersionV0{[8]byte{'b', 'e', 't', 'a', '.', '1', '2', '3'}, 1, 0, 0, 0}.Encode(), "v1.0.0+beta.123"},
		{ProtocolVersionV0{[8]byte{'a', 'b', 1}, 42, 0, 2, 0}.Encode(), "v42.0.2+0x6162010000000000"}, // do not render invalid alpha numeric
		{ProtocolVersionV0{[8]byte{1, 2, 3, 4, 5, 6, 7, 8}, 42, 0, 2, 0}.Encode(), "v42.0.2+0x0102030405060708"},
	}
	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			got := tc.version.String()
			if got != tc.expected {
				t.Fatalf("got %q but expected %q", got, tc.expected)
			}
		})
	}
}

type hardforkConfig struct {
	chainID                  uint64
	ShanghaiTime             uint64
	CancunTime               uint64
	BedrockBlock             *big.Int
	RegolithTime             uint64
	CanyonTime               uint64
	EcotoneTime              uint64
	FjordTime                uint64
	GraniteTime              uint64
	HoloceneTime             uint64
	EIP1559Elasticity        uint64
	EIP1559Denominator       uint64
	EIP1559DenominatorCanyon uint64
}

var bobaSepoliaCfg = hardforkConfig{
	chainID:                  28882,
	ShanghaiTime:             uint64(1705600788),
	CancunTime:               uint64(1709078400),
	BedrockBlock:             big.NewInt(511),
	RegolithTime:             uint64(1705600788),
	CanyonTime:               uint64(1705600788),
	EcotoneTime:              uint64(1709078400),
	FjordTime:                uint64(1722297600),
	GraniteTime:              uint64(1726470000),
	HoloceneTime:             uint64(1736150400),
	EIP1559Elasticity:        6,
	EIP1559Denominator:       50,
	EIP1559DenominatorCanyon: 250,
}

var bobaMainnetCfg = hardforkConfig{
	chainID:                  288,
	ShanghaiTime:             uint64(1713302879),
	CancunTime:               uint64(1713302880),
	BedrockBlock:             big.NewInt(1149019),
	RegolithTime:             uint64(1713302879),
	CanyonTime:               uint64(1713302879),
	EcotoneTime:              uint64(1713302880),
	FjordTime:                uint64(1725951600),
	GraniteTime:              uint64(1729753200),
	HoloceneTime:             uint64(1738785600),
	EIP1559Elasticity:        6,
	EIP1559Denominator:       50,
	EIP1559DenominatorCanyon: 250,
}

var opSepoliaCfg = hardforkConfig{
	chainID:                  11155420,
	ShanghaiTime:             uint64(1699981200),
	CancunTime:               uint64(1708534800),
	BedrockBlock:             big.NewInt(0),
	RegolithTime:             uint64(0),
	CanyonTime:               uint64(1699981200),
	EcotoneTime:              uint64(1708534800),
	FjordTime:                uint64(1716998400),
	GraniteTime:              uint64(1723478400),
	HoloceneTime:             uint64(1732633200),
	EIP1559Elasticity:        6,
	EIP1559Denominator:       50,
	EIP1559DenominatorCanyon: 250,
}

var opMainnetCfg = hardforkConfig{
	chainID:                  10,
	ShanghaiTime:             uint64(1704992401),
	CancunTime:               uint64(1710374401),
	BedrockBlock:             big.NewInt(105235063),
	RegolithTime:             uint64(0),
	CanyonTime:               uint64(1704992401),
	EcotoneTime:              uint64(1710374401),
	FjordTime:                uint64(1720627201),
	GraniteTime:              uint64(1726070401),
	HoloceneTime:             uint64(1736445601),
	EIP1559Elasticity:        6,
	EIP1559Denominator:       50,
	EIP1559DenominatorCanyon: 250,
}

func TestChainConfigByOpStackChainName(t *testing.T) {
	hardforkConfigsByName := map[uint64]hardforkConfig{
		28882:    bobaSepoliaCfg,
		288:      bobaMainnetCfg,
		11155420: opSepoliaCfg,
		10:       opMainnetCfg,
	}

	for name, expectedHarhardforkCfg := range hardforkConfigsByName {
		chain, err := superchain.GetChain(name)
		require.NoError(t, err)
		chainConf, err := chain.Config()
		require.NoError(t, err)
		gotCfg, err := LoadOPStackChainConfig(chainConf)
		require.NotNil(t, gotCfg)
		require.NoError(t, err)

		// ChainID
		require.Equal(t, expectedHarhardforkCfg.chainID, gotCfg.ChainID.Uint64())

		// Hardforks
		require.Equal(t, expectedHarhardforkCfg.ShanghaiTime, *gotCfg.ShanghaiTime)
		require.Equal(t, expectedHarhardforkCfg.CancunTime, *gotCfg.CancunTime)
		require.Equal(t, expectedHarhardforkCfg.BedrockBlock, gotCfg.BedrockBlock)
		require.Equal(t, expectedHarhardforkCfg.RegolithTime, *gotCfg.RegolithTime)
		require.Equal(t, expectedHarhardforkCfg.CanyonTime, *gotCfg.CanyonTime)
		require.Equal(t, expectedHarhardforkCfg.EcotoneTime, *gotCfg.EcotoneTime)
		require.Equal(t, expectedHarhardforkCfg.FjordTime, *gotCfg.FjordTime)
		require.Equal(t, expectedHarhardforkCfg.GraniteTime, *gotCfg.GraniteTime)
		require.Equal(t, expectedHarhardforkCfg.HoloceneTime, *gotCfg.HoloceneTime)

		// EIP-1559
		require.Equal(t, expectedHarhardforkCfg.EIP1559Elasticity, gotCfg.Optimism.EIP1559Elasticity)
		require.Equal(t, expectedHarhardforkCfg.EIP1559Denominator, gotCfg.Optimism.EIP1559Denominator)
		require.Equal(t, expectedHarhardforkCfg.EIP1559DenominatorCanyon, *gotCfg.Optimism.EIP1559DenominatorCanyon)
	}
}
