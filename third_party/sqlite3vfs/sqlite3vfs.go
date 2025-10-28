package sqlite3vfs

import "time"

// Register a VFS with sqlite. The name specified must be unique and
// should match the name given when opening the database:
// `?vfs={{name}}`.
func RegisterVFS(name string, vfs VFS, opts ...Option) error {
	var vfsOpts options

	for _, opt := range opts {
		err := opt.setOption(&vfsOpts)
		if err != nil {
			return err
		}
	}

	extVFS, ok := vfs.(ExtendedVFSv1)
	if !ok {
		extVFS = &defaultVFSv1{vfs}
	}

	maxPathName := vfsOpts.maxPathName
	if maxPathName == 0 {
		maxPathName = 1024
	}

	return newVFS(name, extVFS, maxPathName)
}

type VFS interface {
	// Open a file.
	// Name will either be the name of the file to open or "" for a temp file.
	Open(name string, flags OpenFlag) (File, OpenFlag, error)

	// Delete the named file. If dirSync is true them ensure the file-system
	// modification has been synced to disk before returning.
	Delete(name string, dirSync bool) error

	// Test for access permission. Returns true if the requested permission is available.
	Access(name string, flags AccessFlag) (bool, error)

	// FullPathname returns the canonicalized version of name.
	FullPathname(name string) string
}

type ExtendedVFSv1 interface {
	VFS

	// Randomness populates n with pseudo-random data.
	// Returns the number of bytes of randomness obtained.
	Randomness(n []byte) int

	// Sleep for duration
	Sleep(d time.Duration)

	CurrentTime() time.Time
}

type OpenFlag int

const (
	OpenReadOnly      OpenFlag = 0x00000001
	OpenReadWrite     OpenFlag = 0x00000002
	OpenCreate        OpenFlag = 0x00000004
	OpenDeleteOnClose OpenFlag = 0x00000008
	OpenExclusive     OpenFlag = 0x00000010
	OpenAutoProxy     OpenFlag = 0x00000020
	OpenURI           OpenFlag = 0x00000040
	OpenMemory        OpenFlag = 0x00000080
	OpenMainDB        OpenFlag = 0x00000100
	OpenTempDB        OpenFlag = 0x00000200
	OpenTransientDB   OpenFlag = 0x00000400
	OpenMainJournal   OpenFlag = 0x00000800
	OpenTempJournal   OpenFlag = 0x00001000
	OpenSubJournal    OpenFlag = 0x00002000
	OpenSuperJournal  OpenFlag = 0x00004000
	OpenNoMutex       OpenFlag = 0x00008000
	OpenFullMutex     OpenFlag = 0x00010000
	OpenSharedCache   OpenFlag = 0x00020000
	OpenPrivateCache  OpenFlag = 0x00040000
	OpenWAL           OpenFlag = 0x00080000
	OpenNoFollow      OpenFlag = 0x01000000
)

type AccessFlag int

const (
	AccessExists    AccessFlag = 0 // Does the file exist?
	AccessReadWrite AccessFlag = 1 // Is the file both readable and writeable?
	AccessRead      AccessFlag = 2 // Is the file readable?

)
