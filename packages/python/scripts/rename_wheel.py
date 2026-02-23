#!/usr/bin/env python3
"""Rename a wheel file with the correct platform tag.

Usage: python rename_wheel.py <wheel_dir> <platform_tag>
  platform_tag examples: manylinux_2_35_x86_64, manylinux_2_35_aarch64,
                         macosx_11_0_x86_64, macosx_11_0_arm64
"""
import glob
import os
import sys


def main():
    wheel_dir = sys.argv[1]
    platform_tag = sys.argv[2]

    wheels = glob.glob(os.path.join(wheel_dir, "*.whl"))
    if not wheels:
        print(f"No wheels found in {wheel_dir}", file=sys.stderr)
        sys.exit(1)

    for wheel in wheels:
        parts = os.path.basename(wheel).split("-")
        # Wheel filename: {name}-{ver}-{python}-{abi}-{platform}.whl
        parts[-1] = f"{platform_tag}.whl"
        parts[-2] = "none"
        parts[-3] = "cp38.cp39.cp310.cp311.cp312.cp313"
        new_name = "-".join(parts)
        new_path = os.path.join(wheel_dir, new_name)
        os.rename(wheel, new_path)
        print(f"Renamed: {os.path.basename(wheel)} -> {new_name}")


if __name__ == "__main__":
    main()
