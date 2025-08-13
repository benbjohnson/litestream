#!/usr/bin/env python3
import sys
import os
import time
from moto.server import ThreadedMotoServer
import boto3
import subprocess

cmd = sys.argv[1:]
if len(cmd) == 0:
    print(f"usage: {sys.argv[0]} <command> [arguments]", file=sys.stderr)
    sys.exit(1)

env = os.environ.copy() | {
    "LITESTREAM_S3_ACCESS_KEY_ID": "lite",
    "LITESTREAM_S3_SECRET_ACCESS_KEY": "stream",
    "LITESTREAM_S3_BUCKET": f"test{int(time.time())}",
    "LITESTREAM_S3_ENDPOINT": "http://127.0.0.1:5000",
    "LITESTREAM_S3_FORCE_PATH_STYLE": "true",
}

server = ThreadedMotoServer()
server.start()

s3 = boto3.client(
    "s3",
    aws_access_key_id=env["LITESTREAM_S3_ACCESS_KEY_ID"],
    aws_secret_access_key=env["LITESTREAM_S3_SECRET_ACCESS_KEY"],
    endpoint_url=env["LITESTREAM_S3_ENDPOINT"]
).create_bucket(Bucket=env["LITESTREAM_S3_BUCKET"])

proc = subprocess.run(cmd, env=env)

server.stop()
sys.exit(proc.returncode)
