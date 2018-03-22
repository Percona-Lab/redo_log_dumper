package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
)

type Header struct {
	GroupNumber           uint32
	FirstLSN              uint64 // First Log Sequence Number
	ArchivedLogFileNumber uint32
	_                     [32]byte  // ibbackup & creation time
	_                     [464]byte // Padding
}

type Checkpoint struct {
	Number      uint64    // 0x00 8 Log checkpoint number
	LSN         uint64    // 0x08 8 Log sequence number of checkpoint
	Offset      uint32    // 0x10 4 Offset to the log entry, calculated by log_group_calc_lsn_offset() [19]
	BufferSize  uint32    // 0x14 4 Size of the buffer (a fixed value: 2 · 1024 · 1024)
	ArchivedLSN uint64    // 0x18 8 Archived log sequence number. If UNIV_LOG_ARCHIVE is not activated, InnoDB inserts FF FF FF FF FF FF FF FF here.
	_           [256]byte // 0x20 256 Spacing and padding
	Checksum1   uint32    // 0x120 4 Checksum 1 (validating the contents from offset 0x00 to 0x19F)
	Checksum2   uint32    // 0x124 4 Checksum 2 (validating the block without the log sequence number, but including checksum 1, i.e. values from 0x08 to0x124)
	CuurentFSP  uint32    // 0x128 4 Current fsp free limit in tablespace 0, given in units of one megabyte; used by ibbackup to decide if unused ends of non-auto-extending data files in space 0 can be truncated [20]
	Magic       uint32    // 0x12C 4 Magic number that tells if the checkpoint contains the field above (added to InnoDB version 3.23.50 [20])
	_           [208]byte // Padding
}

type LogBlock struct {
	HeaderNumber            uint32 // 0x00 Log block header number. If the most significant bit is 1, the following block is the first block in a log flush write segment. [20].
	BlockSize               uint16 // Number of bytes written to this block.
	Offset                  uint16 // Offset to the first start of a log record group of this block (see II-D3 for further details).
	CurrentActiveCheckpoint uint32 // Number of the currently active checkpoint (see II-C).
	HdrSize                 uint16 // Hdr-size
	Data                    [494]byte
	Checksum                uint32 //  Checksum of the log block contents. In InnoDB versions 3.23.52 or earlier this did not contain the checksum but the same value as LOG_BLOCK_HDR_NO [20].  Table IV
}

func main() {
	path := "/home/karl/sandboxes/msb_5_7_21/data/ib_logfile0"

	file, err := os.Open(path)
	if err != nil {
		log.Fatal("Error while opening file", err)
	}

	defer file.Close()

	fmt.Printf("%s opened\n", path)

	header := Header{}
	err = readIntoStruct(file, &header, 512)

	firstBlockMask := uint32(0x80000000)

	fmt.Println("================================================================================")
	fmt.Println("Parsed header data:")
	fmt.Printf("Group Number            : %d\n", header.GroupNumber)
	fmt.Printf("First LSN               : %d\n", header.FirstLSN)
	fmt.Printf("Archived Log File Number: %d\n", header.ArchivedLogFileNumber)

	checkpoint := Checkpoint{}
	const cpsize = 512
	err = readIntoStruct(file, &checkpoint, cpsize)
	fmt.Println("================================================================================")
	fmt.Println("Parsed first checkpoint data:")
	fmt.Printf("Number     : 0x%X\n", checkpoint.Number)
	fmt.Printf("LSN        : 0x%X\n", checkpoint.LSN)
	fmt.Printf("Offset     : 0x%d\n", checkpoint.Offset)
	fmt.Printf("BufferSize : %d\n", checkpoint.BufferSize)
	fmt.Printf("ArchivedLSN: 0x%X\n", checkpoint.ArchivedLSN)
	fmt.Printf("Checksum1  : 0x%X\n", checkpoint.Checksum1)
	fmt.Printf("Checksum2  : 0x%X\n", checkpoint.Checksum2)
	fmt.Printf("CurentFSP  : 0x%X\n", checkpoint.CuurentFSP)
	fmt.Printf("Magic      : 0x%X\n", checkpoint.Magic)

	checkpoint2 := Checkpoint{}
	err = readIntoStruct(file, &checkpoint2, cpsize)
	fmt.Println("================================================================================")
	fmt.Println("Parsed second checkpoint data:")
	fmt.Printf("Number     : 0x%X\n", checkpoint2.Number)
	fmt.Printf("LSN        : 0x%X\n", checkpoint2.LSN)
	fmt.Printf("Offset     : 0x%X\n", checkpoint2.Offset)
	fmt.Printf("BufferSize : %d\n", checkpoint2.BufferSize)
	fmt.Printf("ArchivedLSN: 0x%X\n", checkpoint2.ArchivedLSN)
	fmt.Printf("Checksum1  : 0x%X\n", checkpoint2.Checksum1)
	fmt.Printf("Checksum2  : 0x%X\n", checkpoint2.Checksum2)
	fmt.Printf("CurentFSP  : 0x%X\n", checkpoint2.CuurentFSP)
	fmt.Printf("Magic      : 0x%X\n", checkpoint2.Magic)
	fmt.Println()
	fmt.Println()

	// Move to the start of the logs
	// Current position is 512 + 512 + 512 = 1536 and logs start at 2048
	if pos, err := file.Seek(512, io.SeekCurrent); err == nil {
		fmt.Printf("Current position: %d\n", pos)
	}
	for i := 0; err == nil; i++ {
		logBlock := LogBlock{}
		err = readIntoStruct(file, &logBlock, 512)
		fmt.Printf("Header number     : 0x%X\n", logBlock.HeaderNumber)
		fmt.Printf("Is first block    : %v\n", logBlock.HeaderNumber&firstBlockMask == firstBlockMask)
		fmt.Printf("Size              : %d\n", logBlock.BlockSize)
		fmt.Printf("Offset            : %d\n", logBlock.Offset)
		fmt.Printf("Current checkpoint: 0x%X\n", logBlock.CurrentActiveCheckpoint)
		fmt.Printf("Hdr-size          : %d\n", logBlock.HdrSize)
		fmt.Printf("Checksum          : 0x%0X\n", logBlock.Checksum)
		fmt.Printf("Data:\n% X\n", logBlock.Data)
		fmt.Println()
	}

}

func readIntoStruct(file *os.File, dest interface{}, size int) error {
	data, err := readNextBytes(file, int(size))
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(data)
	err = binary.Read(buffer, binary.BigEndian, dest)
	if err != nil {
		return errors.Wrap(err, "binary.Read failed")
	}

	return nil
}

func readNextBytes(file *os.File, number int) ([]byte, error) {
	bytes := make([]byte, number)

	_, err := file.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
