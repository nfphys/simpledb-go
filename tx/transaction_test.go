package tx_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
	"github.com/nfphys/simpledb-go/tx/recovery"
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

func TestCommit(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	tx := tx.NewTransaction(fm, lm, bm)

	blk := file.NewBlockId("testfile", 0)
	p := file.NewPage(blocksize)
	p.SetInt(0, 0)
	fm.Write(blk, p)

	err := tx.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx.SetInt(blk, 0, 42, true)

	// When
	tx.Commit()

	// Then
	// Check if the block is flushed to disk
	p = file.NewPage(blocksize)
	fm.Read(blk, p)
	if p.GetInt(0) != 42 {
		t.Errorf("Expected 42, got %d", p.GetInt(0))
	}

	// Check if the log is created
	recs := []recovery.LogRecord{}
	for rec := range lm.Iterator() {
		recs = append(recs, recovery.CreateLogRecord(rec))
	}
	if len(recs) != 3 {
		t.Errorf("Expected 3 log records, got %d", len(recs))
	}
	if recs[0].Op() != recovery.COMMIT {
		t.Errorf("Expected COMMIT, got %d", recs[0].Op())
	}
	if recs[1].Op() != recovery.SETINT {
		t.Errorf("Expected SETINT, got %d", recs[1].Op())
	}
	if recs[2].Op() != recovery.START {
		t.Errorf("Expected START, got %d", recs[2].Op())
	}
}

func TestRollback(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	tx := tx.NewTransaction(fm, lm, bm)

	blk := file.NewBlockId("testfile", 0)
	p := file.NewPage(blocksize)
	p.SetInt(0, 0)
	fm.Write(blk, p)

	err := tx.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx.SetInt(blk, 0, 42, true)

	// When
	tx.Rollback()

	// Then
	// Check if the block is rolled back
	p = file.NewPage(blocksize)
	fm.Read(blk, p)
	if p.GetInt(0) != 0 {
		t.Errorf("Expected 0, got %d", p.GetInt(0))
	}

	// Check if the log is created
	recs := []recovery.LogRecord{}
	for rec := range lm.Iterator() {
		recs = append(recs, recovery.CreateLogRecord(rec))
	}
	if len(recs) != 3 {
		t.Errorf("Expected 3 log records, got %d", len(recs))
	}
	if recs[0].Op() != recovery.ROLLBACK {
		t.Errorf("Expected ROLLBACK, got %d", recs[0].Op())
	}
	if recs[1].Op() != recovery.SETINT {
		t.Errorf("Expected SETINT, got %d", recs[1].Op())
	}
	if recs[2].Op() != recovery.START {
		t.Errorf("Expected START, got %d", recs[2].Op())
	}
}
