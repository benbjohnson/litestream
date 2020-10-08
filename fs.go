package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.FS = (*FS)(nil)

type FS struct {
	SourcePath string
}

// Root returns the file system root.
func (f *FS) Root() (fs.Node, error) {
	return &File{fs: f}, nil
}

var _ fs.Node = (*File)(nil)

type File struct {
	fs   *FS    // base filesystem
	path string // path within file system
}

func (f *File) srcpath() string {
	return filepath.Join(f.fs.SourcePath, f.path)
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fi, err := os.Stat(f.srcpath())
	if err != nil {
		return err
	}
	statt := fi.Sys().(*syscall.Stat_t)

	// TODO: Cache attr w/ a.Valid?

	if f.path == "" {
		a.Inode = 1
	} else {
		a.Inode = statt.Ino
	}
	a.Size = uint64(fi.Size())
	a.Blocks = uint64(statt.Blocks)
	a.Atime = time.Unix(statt.Atim.Sec, statt.Atim.Nsec).UTC()
	a.Mtime = time.Unix(statt.Mtim.Sec, statt.Mtim.Nsec).UTC()
	a.Ctime = time.Unix(statt.Ctim.Sec, statt.Ctim.Nsec).UTC()
	a.Mode = fi.Mode()
	a.Nlink = uint32(statt.Nlink)
	a.Uid = uint32(statt.Uid)
	a.Gid = uint32(statt.Gid)
	a.Rdev = uint32(statt.Rdev)
	a.BlockSize = uint32(statt.Blksize)

	return nil
}

func (f *File) Lookup(ctx context.Context, name string) (fs.Node, error) {
	path := filepath.Join(f.path, name)
	srcpath := filepath.Join(f.fs.SourcePath, path)
	if _, err := os.Stat(srcpath); os.IsNotExist(err) {
		return nil, syscall.ENOENT
	}
	return &File{fs: f.fs, path: path}, nil
}

func (f *File) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fis, err := ioutil.ReadDir(f.srcpath())
	if err != nil {
		return nil, err
	}

	ents := make([]fuse.Dirent, len(fis))
	for i, fi := range fis {
		statt := fi.Sys().(*syscall.Stat_t)
		ents[i] = fuse.Dirent{Inode: statt.Ino, Name: fi.Name()}
	}
	return ents, nil
}
