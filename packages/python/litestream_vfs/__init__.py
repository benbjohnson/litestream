"""Litestream VFS extension for SQLite."""

import os
import sys

_EXT_MAP = {
    "linux": "litestream-vfs.so",
    "darwin": "litestream-vfs.dylib",
}


def loadable_path():
    """Return the filesystem path to the loadable VFS extension."""
    platform = sys.platform
    if platform.startswith("linux"):
        platform = "linux"
    filename = _EXT_MAP.get(platform)
    if filename is None:
        raise OSError(f"Unsupported platform: {sys.platform}")
    path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"VFS extension not found at {path}")
    return path


def load(conn):
    """Load the Litestream VFS extension into a sqlite3 connection."""
    path = loadable_path()
    conn.enable_load_extension(True)
    try:
        conn.load_extension(os.path.splitext(path)[0])
    finally:
        conn.enable_load_extension(False)
