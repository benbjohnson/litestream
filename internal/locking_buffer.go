package internal

import (
	"bytes"
	"io"
	"sync"
)

// LockingBuffer wraps a bytes.Buffer with a mutex.
type LockingBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *LockingBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	buf := b.b.Bytes()
	other := make([]byte, len(buf))
	copy(other, buf)
	return other
}

func (b *LockingBuffer) Cap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Cap()
}

func (b *LockingBuffer) Grow(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b.Grow(n)
}

func (b *LockingBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Len()
}

func (b *LockingBuffer) Next(n int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	buf := b.b.Next(n)
	other := make([]byte, len(buf))
	copy(other, buf)
	return other
}

func (b *LockingBuffer) Read(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Read(p)
}

func (b *LockingBuffer) ReadByte() (byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.ReadByte()
}

func (b *LockingBuffer) ReadBytes(delim byte) (line []byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.ReadBytes(delim)
}

func (b *LockingBuffer) ReadFrom(r io.Reader) (n int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.ReadFrom(r)
}

func (b *LockingBuffer) ReadRune() (r rune, size int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.ReadRune()
}

func (b *LockingBuffer) ReadString(delim byte) (line string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.ReadString(delim)
}

func (b *LockingBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b.Reset()
}

func (b *LockingBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func (b *LockingBuffer) Truncate(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.b.Truncate(n)
}

func (b *LockingBuffer) UnreadByte() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.UnreadByte()
}

func (b *LockingBuffer) UnreadRune() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.UnreadRune()
}

func (b *LockingBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *LockingBuffer) WriteByte(c byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.WriteByte(c)
}

func (b *LockingBuffer) WriteRune(r rune) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.WriteRune(r)
}

func (b *LockingBuffer) WriteString(s string) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.WriteString(s)
}

func (b *LockingBuffer) WriteTo(w io.Writer) (n int64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.WriteTo(w)
}
