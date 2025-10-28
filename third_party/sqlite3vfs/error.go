package sqlite3vfs

import "fmt"

type sqliteError struct {
	code int
	text string
}

func (e sqliteError) Error() string {
	return fmt.Sprintf("sqlite (%d) %s", e.code, e.text)
}

// https://www.sqlite.org/rescode.html

const (
	sqliteOK = 0
)

var (
	GenericError    = sqliteError{1, "Generic Error"}
	InternalError   = sqliteError{2, "Internal Error"}
	PermError       = sqliteError{3, "Perm Error"}
	AbortError      = sqliteError{4, "Abort Error"}
	BusyError       = sqliteError{5, "Busy Error"}
	LockedError     = sqliteError{6, "Locked Error"}
	NoMemError      = sqliteError{7, "No Mem Error"}
	ReadOnlyError   = sqliteError{8, "Read Only Error"}
	InterruptError  = sqliteError{9, "Interrupt Error"}
	IOError         = sqliteError{10, "IO Error"}
	CorruptError    = sqliteError{11, "Corrupt Error"}
	NotFoundError   = sqliteError{12, "Not Found Error"}
	FullError       = sqliteError{13, "Full Error"}
	CantOpenError   = sqliteError{14, "CantOpen Error"}
	ProtocolError   = sqliteError{15, "Protocol Error"}
	EmptyError      = sqliteError{16, "Empty Error"}
	SchemaError     = sqliteError{17, "Schema Error"}
	TooBigError     = sqliteError{18, "TooBig Error"}
	ConstraintError = sqliteError{19, "Constraint Error"}
	MismatchError   = sqliteError{20, "Mismatch Error"}
	MisuseError     = sqliteError{21, "Misuse Error"}
	NoLFSError      = sqliteError{22, "No Large File Support Error"}
	AuthError       = sqliteError{23, "Auth Error"}
	FormatError     = sqliteError{24, "Format Error"}
	RangeError      = sqliteError{25, "Range Error"}
	NotaDBError     = sqliteError{26, "Not a DB Error"}
	NoticeError     = sqliteError{27, "Notice Error"}
	WarningError    = sqliteError{28, "Warning Error"}

	IOErrorRead      = sqliteError{266, "IO Error Read"}
	IOErrorShortRead = sqliteError{522, "IO Error Short Read"}
	IOErrorWrite     = sqliteError{778, "IO Error Write"}
)

var errMap = map[int]sqliteError{
	1:  GenericError,
	2:  InternalError,
	3:  PermError,
	4:  AbortError,
	5:  BusyError,
	6:  LockedError,
	7:  NoMemError,
	8:  ReadOnlyError,
	9:  InterruptError,
	10: IOError,
	11: CorruptError,
	12: NotFoundError,
	13: FullError,
	14: CantOpenError,
	15: ProtocolError,
	16: EmptyError,
	17: SchemaError,
	18: TooBigError,
	19: ConstraintError,
	20: MismatchError,
	21: MisuseError,
	22: NoLFSError,
	23: AuthError,
	24: FormatError,
	25: RangeError,
	26: NotaDBError,
	27: NoticeError,
	28: WarningError,

	266: IOErrorRead,
	522: IOErrorShortRead,
	778: IOErrorWrite,
}

func errFromCode(code int) error {
	if code == 0 {
		return nil
	}
	err, ok := errMap[code]
	if ok {
		return err
	}

	return sqliteError{
		code: code,
		text: "unknown err code",
	}
}
