package internal

import (
	"bytes"
	"fmt"
)

func Hexdump(data []byte) string {
	prevRow := make([]byte, 16)

	var buf bytes.Buffer
	var dupWritten bool
	for i := 0; i < len(data); i += 16 {
		row := make([]byte, 16)
		copy(row, data[i:])

		// Write out one line of asterisks to show that we just have duplicate rows.
		if i != 0 && i+16 < len(data) && bytes.Equal(row, prevRow) {
			if !dupWritten {
				dupWritten = true
				fmt.Fprintln(&buf, "***")
			}
			continue
		}

		// Track previous row so we know when we have duplicates.
		copy(prevRow, row)
		dupWritten = false

		fmt.Fprintf(&buf, "%08x  %02x %02x %02x %02x %02x %02x %02x %02x  %02x %02x %02x %02x %02x %02x %02x %02x  |%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s|\n", i,
			row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
			row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
			toChar(row[0]), toChar(row[1]), toChar(row[2]), toChar(row[3]), toChar(row[4]), toChar(row[5]), toChar(row[6]), toChar(row[7]),
			toChar(row[8]), toChar(row[9]), toChar(row[10]), toChar(row[11]), toChar(row[12]), toChar(row[13]), toChar(row[14]), toChar(row[15]),
		)
	}
	return buf.String()
}

func toChar(b byte) string {
	if b < 32 || b > 126 {
		return "."
	}
	return string(b)
}
