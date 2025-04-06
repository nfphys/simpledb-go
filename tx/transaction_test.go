package tx_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
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

func TestPinAndSetInt(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)

	tx := tx.NewTransaction(fm, lm, bm)

	// When
	err := tx.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx.SetInt(blk, 0, 42, false)

	// Then
	i := tx.GetInt(blk, 0)
	if i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}
}

func TestPinAndSetString(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)

	tx := tx.NewTransaction(fm, lm, bm)

	// When
	err := tx.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx.SetString(blk, 0, "hello", false)

	// Then
	s := tx.GetString(blk, 0)
	if s != "hello" {
		t.Errorf("Expected 'hello', got '%s'", s)
	}
}
