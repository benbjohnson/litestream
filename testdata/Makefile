.PHONY: default
default:
	make -C find-latest-generation/ok
	make -C index-by-timestamp/no-wal
	make -C index-by-timestamp/ok
	make -C index-by-timestamp/snapshot-later-than-wal
	make -C generation-time-bounds/ok
	make -C generation-time-bounds/snapshots-only
	make -C replica-client-time-bounds/ok
	make -C snapshot-time-bounds/ok
	make -C snapshot-index-by-timestamp/ok
	make -C wal-time-bounds/ok
	make -C wal-index-by-timestamp/ok
