package sqlite3vfs

import "time"

func RegisterVFS(name string, vfs VFS, opts ...Option) error {
	var vfsOpts options

	for _, opt := range opts {
		if err := opt.setOption(&vfsOpts); err != nil {
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
	Open(name string, flags OpenFlag) (File, OpenFlag, error)
	Delete(name string, dirSync bool) error
	Access(name string, flags AccessFlag) (bool, error)
	FullPathname(name string) string
}

type FilenameOpener interface {
	OpenFilename(name Filename, flags OpenFlag) (File, OpenFlag, error)
}

type ExtendedVFSv1 interface {
	VFS
	Randomness(n []byte) int
	Sleep(d time.Duration)
	CurrentTime() time.Time
}

type Filename struct {
	name          string
	uriParameters map[string]string
}

func NewFilename(name string, uriParameters map[string]string) Filename {
	f := Filename{name: name}
	if len(uriParameters) != 0 {
		f.uriParameters = make(map[string]string, len(uriParameters))
		for k, v := range uriParameters {
			f.uriParameters[k] = v
		}
	}
	return f
}

func (f Filename) String() string {
	return f.name
}

func (f Filename) URIParameter(name string) (string, bool) {
	v, ok := f.uriParameters[name]
	return v, ok
}

func (f Filename) URIParameters() map[string]string {
	if len(f.uriParameters) == 0 {
		return nil
	}
	m := make(map[string]string, len(f.uriParameters))
	for k, v := range f.uriParameters {
		m[k] = v
	}
	return m
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
	AccessExists    AccessFlag = 0
	AccessReadWrite AccessFlag = 1
	AccessRead      AccessFlag = 2
)
