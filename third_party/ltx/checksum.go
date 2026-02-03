package ltx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"strconv"
	"sync"
)

// Checksum represents an LTX checksum.
type Checksum uint64

// ChecksumPages updates the provided checksums slice with the checksum of each
// page in the specified file. The first (by page number) error encountered is
// returned along with the number of the last page successfully checksummed.
// Checksums for subsequent pages may be updated, regardless of an error being
// returned.
//
// nWorkers specifies the amount of parallelism to use. A reasonable default
// will be used if nWorkers is 0.
func ChecksumPages(dbPath string, pageSize, nPages, nWorkers uint32, checksums []Checksum) (uint32, error) {
	// Based on experimentation on a fly.io machine with a slow SSD, 512Mb is
	// where checksumming starts to take >1s. As the database size increases, we
	// get more benefit from an increasing number of workers. Doing a bunch of
	// benchmarking on fly.io machines of difference sizes with a 1Gb database,
	// 24 threads seems to be the sweet spot.
	if nWorkers == 0 && pageSize*nPages > 512*1024*1024 {
		nWorkers = 24
	}

	if nWorkers <= 1 {
		return checksumPagesSerial(dbPath, 1, nPages, int64(pageSize), checksums)
	}

	perWorker := nPages / nWorkers
	if nPages%nWorkers != 0 {
		perWorker++
	}

	var (
		wg   sync.WaitGroup
		rets = make([]uint32, nWorkers)
		errs = make([]error, nWorkers)
	)

	for w := uint32(0); w < nWorkers; w++ {
		w := w
		firstPage := w*perWorker + 1
		lastPage := firstPage + perWorker - 1
		if lastPage > nPages {
			lastPage = nPages
		}

		wg.Add(1)
		go func() {
			rets[w], errs[w] = checksumPagesSerial(dbPath, firstPage, lastPage, int64(pageSize), checksums)
			wg.Done()
		}()
	}

	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return rets[i], err
		}
	}

	return nPages, nil
}

func checksumPagesSerial(dbPath string, firstPage, lastPage uint32, pageSize int64, checksums []Checksum) (uint32, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return firstPage - 1, err
	}

	_, err = f.Seek(int64(firstPage-1)*pageSize, io.SeekStart)
	if err != nil {
		return firstPage - 1, err
	}

	buf := make([]byte, pageSize+4)
	h := NewHasher()

	for pageNo := firstPage; pageNo <= lastPage; pageNo++ {
		binary.BigEndian.PutUint32(buf, pageNo)

		if _, err := io.ReadFull(f, buf[4:]); err != nil {
			return pageNo - 1, err
		}

		h.Reset()
		_, _ = h.Write(buf)
		checksums[pageNo-1] = ChecksumFlag | Checksum(h.Sum64())
	}

	return lastPage, nil
}

// ChecksumPage returns a CRC64 checksum that combines the page number & page data.
func ChecksumPage(pgno uint32, data []byte) Checksum {
	return ChecksumPageWithHasher(NewHasher(), pgno, data)
}

// ChecksumPageWithHasher returns a CRC64 checksum that combines the page number & page data.
func ChecksumPageWithHasher(h hash.Hash64, pgno uint32, data []byte) Checksum {
	h.Reset()
	_ = binary.Write(h, binary.BigEndian, pgno)
	_, _ = h.Write(data)
	return ChecksumFlag | Checksum(h.Sum64())
}

// ChecksumReader reads an entire database file from r and computes its rolling checksum.
func ChecksumReader(r io.Reader, pageSize int) (Checksum, error) {
	data := make([]byte, pageSize)

	var chksum Checksum
	for pgno := uint32(1); ; pgno++ {
		if _, err := io.ReadFull(r, data); err == io.EOF {
			break
		} else if err != nil {
			return chksum, err
		}
		chksum = ChecksumFlag | (chksum ^ ChecksumPage(pgno, data))
	}
	return chksum, nil
}

// ParseChecksum parses a 16-character hex string into a checksum.
func ParseChecksum(s string) (Checksum, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("invalid formatted checksum length: %q", s)
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid checksum format: %q", s)
	}
	return Checksum(v), nil
}

// String returns c formatted as a fixed-width hex number.
func (c Checksum) String() string {
	return fmt.Sprintf("%016x", uint64(c))
}

func (c Checksum) MarshalJSON() ([]byte, error) {
	return []byte(`"` + c.String() + `"`), nil
}

func (c *Checksum) UnmarshalJSON(data []byte) (err error) {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("cannot unmarshal checksum from JSON value")
	}

	// Set to zero if value is nil.
	if s == nil {
		*c = 0
		return nil
	}

	chksum, err := ParseChecksum(*s)
	if err != nil {
		return fmt.Errorf("cannot parse checksum from JSON string: %q", *s)
	}
	*c = Checksum(chksum)

	return nil
}

// NewHasher returns a new CRC64-ISO hasher.
func NewHasher() hash.Hash64 {
	return crc64.New(crc64.MakeTable(crc64.ISO))
}
