"use strict";

const path = require("path");
const os = require("os");

const PLATFORM_PACKAGES = {
  "darwin-arm64": "litestream-vfs-darwin-arm64",
  "darwin-x64": "litestream-vfs-darwin-amd64",
  "linux-arm64": "litestream-vfs-linux-arm64",
  "linux-x64": "litestream-vfs-linux-amd64",
};

const EXT_MAP = {
  darwin: "litestream-vfs.dylib",
  linux: "litestream-vfs.so",
};

function getLoadablePath() {
  const key = `${os.platform()}-${os.arch()}`;
  const pkg = PLATFORM_PACKAGES[key];
  if (!pkg) {
    throw new Error(`Unsupported platform: ${key}`);
  }

  const ext = EXT_MAP[os.platform()];
  const searchPaths = [
    path.join(process.cwd(), "node_modules"),
    ...module.paths,
  ];
  if (require.main) {
    searchPaths.push(...require.main.paths);
  }
  try {
    const resolved = require.resolve(`${pkg}/package.json`, {
      paths: searchPaths,
    });
    return path.join(path.dirname(resolved), ext);
  } catch {
    throw new Error(
      `Platform package ${pkg} is not installed. ` +
        `Run: npm install ${pkg}`
    );
  }
}

module.exports = { getLoadablePath };
