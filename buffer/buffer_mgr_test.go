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
