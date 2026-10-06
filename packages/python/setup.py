import os
from setuptools import setup, Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(
    name="litestream-vfs",
    version=os.environ.get("LITESTREAM_VERSION", "0.0.0"),
    description="Litestream VFS extension for SQLite",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/benbjohnson/litestream",
    license="Apache-2.0",
    packages=["litestream_vfs"],
    package_data={"litestream_vfs": ["*.so", "*.dylib"]},
    distclass=BinaryDistribution,
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
    ],
)
