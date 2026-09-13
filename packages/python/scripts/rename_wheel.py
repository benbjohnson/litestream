#!/usr/bin/env python3
"""Rename a wheel file with the correct platform tag.

Usage: python rename_wheel.py <wheel_dir> <platform_tag>
  platform_tag examples: manylinux_2_35_x86_64, manylinux_2_35_aarch64,
                         macosx_11_0_x86_64, macosx_11_0_arm64
"""
import glob
import os
import subprocess
import sys


def main():
    wheel_dir = sys.argv[1]
    platform_tag = sys.argv[2]

    wheels = glob.glob(os.path.join(wheel_dir, "*.whl"))
    if not wheels:
        print(f"No wheels found in {wheel_dir}", file=sys.stderr)
        sys.exit(1)

    for wheel in wheels:
        subprocess.run(
            [
                sys.executable, "-m", "wheel", "tags",
                "--python-tag=py3", "--abi-tag=none",
                f"--platform-tag={platform_tag}", "--remove", wheel,
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
