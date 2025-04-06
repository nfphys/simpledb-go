package buffer_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

func setup(blocksize int) *file.FileMgr {
	dbDir := filepath.Join(os.TempDir(), "testdb")
	os.RemoveAll(dbDir)
	return file.NewFileMgr(dbDir, blocksize)
}

func cleanup(fm *file.FileMgr) {
	fm.Close()
	os.RemoveAll(filepath.Join(os.TempDir(), "testdb"))
}

func TestPinOnce(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)

	// When
	buff, err := bm.Pin(blk)

	// Then
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if buff.Block().Equals(blk) != true {
		t.Errorf("Expected buffer block to be %v, got %v", blk, buff.Block())
	}
	if buff.IsPinned() != true {
		t.Errorf("Expected buffer to be pinned, but it is not")
	}
	if bm.Available() != 2 {
		t.Errorf("Expected 2 buffers available, got %d", bm.Available())
	}
}

func TestPinTheSameBlockTwice(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)

	// When
	buff1, _ := bm.Pin(blk)
	buff2, _ := bm.Pin(blk)

	// Then
	if buff1 != buff2 {
		t.Errorf("Expected both buffers to be the same, but they are not")
	}
	if buff1.Block().Equals(blk) != true {
		t.Errorf("Expected buffer block to be %v, got %v", blk, buff1.Block())
	}
	if buff1.IsPinned() != true {
		t.Errorf("Expected buffer to be pinned, but it is not")
	}
	if bm.Available() != 2 {
		t.Errorf("Expected 2 buffers available, got %d", bm.Available())
	}
}

func TestPinDifferentBlocks(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk1 := file.NewBlockId("testfile", 0)
	blk2 := file.NewBlockId("testfile", 1)

	// When
	buff1, _ := bm.Pin(blk1)
	buff2, _ := bm.Pin(blk2)

	// Then
	if buff1 == buff2 {
		t.Errorf("Expected different buffers for different blocks, but they are the same")
	}
	if buff1.Block().Equals(blk1) != true {
		t.Errorf("Expected buffer block to be %v, got %v", blk1, buff1.Block())
	}
	if buff2.Block().Equals(blk2) != true {
		t.Errorf("Expected buffer block to be %v, got %v", blk2, buff2.Block())
	}
	if buff1.IsPinned() != true {
		t.Errorf("Expected buffer to be pinned, but it is not")
	}
	if buff2.IsPinned() != true {
		t.Errorf("Expected buffer to be pinned, but it is not")
	}
	if bm.Available() != 1 {
		t.Errorf("Expected 1 buffers available, got %d", bm.Available())
	}
}

func TestCannotPinMoreThanAvailable(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 1)

	blk1 := file.NewBlockId("testfile", 0)
	blk2 := file.NewBlockId("testfile", 1)
	bm.Pin(blk1)

	// When
	buff2, err := bm.Pin(blk2)

	// Then
	if err != buffer.ErrBufferNotFound {
		t.Errorf("Expected buffer not found error, got %v", err)
	}
	if buff2 != nil {
		t.Errorf("Expected nil buffer for second pin, but got %v", buff2)
	}
}

func TestUnpin(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)
	buff, _ := bm.Pin(blk)

	// When
	bm.Unpin(buff)

	// Then
	if buff.IsPinned() != false {
		t.Errorf("Expected buffer to be unpinned, but it is still pinned")
	}
	if bm.Available() != 3 {
		t.Errorf("Expected 3 buffers available, got %d", bm.Available())
	}
}

func TestFlushAll(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk1 := file.NewBlockId("testfile", 0)
	blk2 := file.NewBlockId("testfile", 1)
	p1 := file.NewPage(blocksize)
	p2 := file.NewPage(blocksize)
	p1.SetString(0, "")
	p2.SetString(0, "")
	fm.Write(blk1, p1)
	fm.Write(blk2, p2)

	buff1, _ := bm.Pin(blk1)
	buff2, _ := bm.Pin(blk2)

	buff1.Contents().SetString(0, "written")
	buff2.Contents().SetString(0, "written")

	tx1 := 1
	tx2 := 2
	buff1.SetModified(tx1, 1)
	buff2.SetModified(tx2, 2)

	// When
	bm.FlushAll(tx1) // flush pages modified by transaction 1

	// Then
	p3 := file.NewPage(blocksize)
	p4 := file.NewPage(blocksize)
	fm.Read(blk1, p3)
	fm.Read(blk2, p4)
	if p3.GetString(0) != "written" {
		t.Errorf("Expected page 1 to be 'written', got '%s'", p3.GetString(0))
	}
	if p4.GetString(0) != "" {
		t.Errorf("Expected page 2 to be '', got '%s'", p4.GetString(0))
	}
	if buff1.ModifiyingTx() != -1 {
		t.Errorf("Expected buffer 1 to be unmodified, got %d", buff1.ModifiyingTx())
	}
	if buff2.ModifiyingTx() != tx2 {
		t.Errorf("Expected buffer 2 to be modified by transaction 2, got %d", buff2.ModifiyingTx())
	}
}
