package sqlite3vfs

import (
	"crypto/rand"
	"time"
)

type defaultVFSv1 struct {
	VFS
}

func (vfs *defaultVFSv1) OpenFilename(name Filename, flags OpenFlag) (File, OpenFlag, error) {
	if opener, ok := vfs.VFS.(FilenameOpener); ok {
		return opener.OpenFilename(name, flags)
	}
	return vfs.VFS.Open(name.String(), flags)
}

func (vfs *defaultVFSv1) Randomness(n []byte) int {
	i, err := rand.Read(n)
	if err != nil {
		panic(err)
	}
	return i
}

func (vfs *defaultVFSv1) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (vfs *defaultVFSv1) CurrentTime() time.Time {
	return time.Now()
}
