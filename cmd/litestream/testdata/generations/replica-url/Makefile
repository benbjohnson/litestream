.PHONY: default
default:
	TZ=UTC touch -ct 200001010000 replica/generations/0000000000000000/snapshots/00000000.snapshot.lz4
	TZ=UTC touch -ct 200001020000 replica/generations/0000000000000000/snapshots/00000001.snapshot.lz4
	TZ=UTC touch -ct 200001010000 replica/generations/0000000000000000/wal/00000000/00000000.wal.lz4
	TZ=UTC touch -ct 200001020000 replica/generations/0000000000000000/wal/00000000/00000001.wal.lz4
	TZ=UTC touch -ct 200001030000 replica/generations/0000000000000000/wal/00000001/00000000.wal.lz4
	TZ=UTC touch -ct 200001020000 replica/generations/0000000000000001/snapshots/00000000.snapshot.lz4

