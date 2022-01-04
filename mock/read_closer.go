package mock

type ReadCloser struct {
	CloseFunc func() error
	ReadFunc  func([]byte) (int, error)
}

func (r *ReadCloser) Close() error {
	return r.CloseFunc()
}

func (r *ReadCloser) Read(b []byte) (int, error) {
	return r.ReadFunc(b)
}
