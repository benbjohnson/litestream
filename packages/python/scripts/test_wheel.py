import email
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from packaging.tags import Tag, compatible_tags
from packaging.utils import parse_wheel_filename
from wheel.wheelfile import WheelFile


PACKAGE_DIR = Path(__file__).resolve().parents[1]
PLATFORMS = (
    "manylinux_2_35_x86_64",
    "manylinux_2_35_aarch64",
    "macosx_11_0_x86_64",
    "macosx_11_0_arm64",
)


class WheelTest(unittest.TestCase):
    def test_release_wheel(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            shutil.copytree(
                PACKAGE_DIR,
                source,
                ignore=shutil.ignore_patterns("build", "dist", "*.egg-info", "__pycache__"),
            )
            payload = b"test SQLite shared library"
            (source / "litestream_vfs" / "litestream-vfs.so").write_bytes(payload)
            subprocess.run(
                [sys.executable, "setup.py", "bdist_wheel"],
                cwd=source,
                env={**os.environ, "LITESTREAM_VERSION": "0.0.0"},
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            original, = (source / "dist").glob("*.whl")
            for platform in PLATFORMS:
                with self.subTest(platform=platform):
                    wheel_dir = root / platform
                    wheel_dir.mkdir()
                    shutil.copy2(original, wheel_dir)
                    subprocess.run(
                        [sys.executable, str(PACKAGE_DIR / "scripts" / "rename_wheel.py"),
                         str(wheel_dir), platform],
                        check=True,
                    )
                    wheel, = wheel_dir.glob("*.whl")
                    _, _, _, tags = parse_wheel_filename(wheel.name)
                    self.assertEqual(tags, {Tag("py3", "none", platform)})
                    for version in ((3, 8), (3, 13), (3, 14), (3, 15)):
                        self.assertTrue(tags.intersection(compatible_tags(version, platforms=[platform])))
                    with WheelFile(wheel) as archive:
                        metadata_path, = (name for name in archive.namelist() if name.endswith(".dist-info/WHEEL"))
                        metadata = email.message_from_bytes(archive.read(metadata_path))
                        self.assertEqual(metadata.get_all("Tag"), [f"py3-none-{platform}"])
                        self.assertEqual(metadata["Root-Is-Purelib"], "false")
                        self.assertFalse(any("_noop" in name for name in archive.namelist()))
                        self.assertEqual(archive.read("litestream_vfs/litestream-vfs.so"), payload)
                        for name in archive.namelist():
                            archive.read(name)
                    print(f"Verified: {wheel.name}")

    def test_empty_wheel_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [sys.executable, str(PACKAGE_DIR / "scripts" / "rename_wheel.py"), tmp, PLATFORMS[0]],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("No wheels found", result.stderr)


if __name__ == "__main__":
    unittest.main()
