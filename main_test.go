package main

import (
	"encoding/binary"
	"testing"

	"github.com/Percona-Lab/pt-mysql-config-diff/testutils"
)

func TestSizes(t *testing.T) {
	header := Header{}
	headerSize := binary.Size(header)

	testutils.Equals(t, headerSize, 512)
}
