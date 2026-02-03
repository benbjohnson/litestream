package ltx

import (
	"bytes"
	"fmt"
	"io"
)

// FileSpec is an in-memory representation of an LTX file. Typically used for testing.
type FileSpec struct {
	Header  Header
	Pages   []PageSpec
	Trailer Trailer
}

// Write encodes a file spec to a file.
func (s *FileSpec) WriteTo(dst io.Writer) (n int64, err error) {
	enc, err := NewEncoder(dst)
	if err != nil {
		return 0, fmt.Errorf("create ltx encoder: %w", err)
	}
	if err := enc.EncodeHeader(s.Header); err != nil {
		return 0, fmt.Errorf("encode header: %s", err)
	}

	for i, page := range s.Pages {
		if err := enc.EncodePage(page.Header, page.Data); err != nil {
			return 0, fmt.Errorf("encode page[%d]: %s", i, err)
		}
	}

	enc.SetPostApplyChecksum(s.Trailer.PostApplyChecksum)

	if err := enc.Close(); err != nil {
		return 0, fmt.Errorf("close encoder: %s", err)
	}

	// Update checksums.
	s.Trailer = enc.Trailer()

	return enc.N(), nil
}

// ReadFromFile encodes a file spec to a file. Always return n of zero.
func (s *FileSpec) ReadFrom(src io.Reader) (n int64, err error) {
	dec := NewDecoder(src)

	// Read header frame and initialize spec slices to correct size.
	if err := dec.DecodeHeader(); err != nil {
		return 0, fmt.Errorf("read header: %s", err)
	}
	s.Header = dec.Header()

	// Read page frames.
	for {
		page := PageSpec{Data: make([]byte, s.Header.PageSize)}
		if err := dec.DecodePage(&page.Header, page.Data); err == io.EOF {
			break
		} else if err != nil {
			return 0, fmt.Errorf("read page header: %s", err)
		}

		s.Pages = append(s.Pages, page)
	}

	if err := dec.Close(); err != nil {
		return 0, fmt.Errorf("close reader: %s", err)
	}
	s.Trailer = dec.Trailer()

	return int64(dec.N()), nil
}

// GoString returns the Go representation of s.
func (s *FileSpec) GoString() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "{\n")
	fmt.Fprintf(&buf, "\tHeader: %#v,\n", s.Header)

	if s.Pages == nil {
		fmt.Fprintf(&buf, "\tPages: nil,\n")
	} else {
		fmt.Fprintf(&buf, "\tPages: []*PageSpec{,\n")
		for i := range s.Pages {
			fmt.Fprintf(&buf, "\t\t%#v,\n", &s.Pages[i])
		}
		fmt.Fprintf(&buf, "\t},\n")
	}

	fmt.Fprintf(&buf, "\tTrailer: %#v,\n", s.Trailer)
	fmt.Fprintf(&buf, "}")

	return buf.String()
}

// PageSpec is an in-memory representation of an LTX page frame. Typically used for testing.
type PageSpec struct {
	Header PageHeader
	Data   []byte
}

// GoString returns the Go representation of s.
func (s *PageSpec) GoString() string {
	var data string
	if len(s.Data) < 4 {
		data = fmt.Sprintf(`"%x"`, s.Data)
	} else {
		data = fmt.Sprintf(`"%x..."`, s.Data[:4])
	}
	return fmt.Sprintf(`{Header:%#v, Data:[]byte(%s)}`, s.Header, data)
}
