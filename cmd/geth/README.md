## init
```azure
~/go/bin/geth --datadir=op_geth --gcmode=archive init --state.scheme=hash /Users/yangweitao/dev/okx/xlayer-erigon/test-pp-op/config-op/genesis.json
~/go/bin/geth --datadir=op_geth verify-genesis /Users/yangweitao/dev/okx/xlayer-erigon/test-pp-op/config-op/genesis.json --ignore-addresses "0x000000000000000000000000000000005ca1ab1e"
geth --datadir=/mnt/ramdisk_op/op_geth_data verify-genesis /mnt/ramdisk_op/genesis.json --ignore-addresses "0x000000000000000000000000000000005ca1ab1e"

```