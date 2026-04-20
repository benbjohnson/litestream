package sqlite3vfs

type options struct {
	maxPathName int
}

type Option interface {
	setOption(*options) error
}

type maxPathOption struct {
	maxPath int
}

func (o maxPathOption) setOption(opts *options) error {
	opts.maxPathName = o.maxPath
	return nil
}

func WithMaxPathName(n int) Option {
	return maxPathOption{maxPath: n}
}
