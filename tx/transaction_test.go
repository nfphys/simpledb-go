package tx_test

import (
	"fmt"
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

	tx1 := tx.NewTransaction(fm, lm, bm)

	// When
	err := tx1.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx1.SetInt(blk, 0, 42)

	// Then
	i := tx1.GetInt(blk, 0)
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

	tx1 := tx.NewTransaction(fm, lm, bm)

	// When
	err := tx1.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx1.SetString(blk, 0, "hello")

	// Then
	s := tx1.GetString(blk, 0)
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

	blk := file.NewBlockId("testfile", 0)
	p := file.NewPage(blocksize)
	p.SetInt(0, 0)
	p.SetString(100, "")
	fm.Write(blk, p)

	tx1 := tx.NewTransaction(fm, lm, bm)
	err := tx1.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx1.SetInt(blk, 0, 42)
	tx1.SetString(blk, 100, "hello")

	// When
	tx1.Commit()

	// Then
	// Check if the block is flushed to disk
	p = file.NewPage(blocksize)
	fm.Read(blk, p)
	if p.GetInt(0) != 42 {
		t.Errorf("Expected 42, got %d", p.GetInt(0))
	}
	if p.GetString(100) != "hello" {
		t.Errorf("Expected 'hello', got '%s'", p.GetString(100))
	}

	// Check if the log is created
	recs := []tx.LogRecord{}
	for rec := range lm.Iterator() {
		recs = append(recs, tx.CreateLogRecord(rec))
	}
	fmt.Println(recs[0].Op(), recs[1].Op())
	if len(recs) != 4 {
		t.Errorf("Expected 3 log records, got %d", len(recs))
		return
	}
	if recs[0].Op() != tx.COMMIT {
		t.Errorf("Expected COMMIT, got %d", recs[0].Op())
	}
	if recs[1].Op() != tx.SETSTRING {
		t.Errorf("Expected SETSTRING, got %d", recs[1].Op())
	}
	if recs[2].Op() != tx.SETINT {
		t.Errorf("Expected SETINT, got %d", recs[2].Op())
	}
	if recs[3].Op() != tx.START {
		t.Errorf("Expected START, got %d", recs[3].Op())
	}
}

func TestRollback(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")
	bm := buffer.NewBufferMgr(fm, lm, 3)

	blk := file.NewBlockId("testfile", 0)
	p := file.NewPage(blocksize)
	p.SetInt(0, 0)
	p.SetString(100, "")
	fm.Write(blk, p)

	tx1 := tx.NewTransaction(fm, lm, bm)
	err := tx1.Pin(blk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	tx1.SetInt(blk, 0, 42)
	tx1.SetString(blk, 100, "hello")

	// When
	tx1.Rollback()

	// Then
	// Check if the block is rolled back
	p = file.NewPage(blocksize)
	fm.Read(blk, p)
	if p.GetInt(0) != 0 {
		t.Errorf("Expected 0, got %d", p.GetInt(0))
	}
	if p.GetString(100) != "" {
		t.Errorf("Expected '', got '%s'", p.GetString(100))
	}

	// Check if the log is created
	recs := []tx.LogRecord{}
	for rec := range lm.Iterator() {
		recs = append(recs, tx.CreateLogRecord(rec))
	}
	if len(recs) != 4 {
		t.Errorf("Expected 3 log records, got %d", len(recs))
		return
	}
	if recs[0].Op() != tx.ROLLBACK {
		t.Errorf("Expected ROLLBACK, got %d", recs[0].Op())
	}
	if recs[1].Op() != tx.SETSTRING {
		t.Errorf("Expected SETSTRING, got %d", recs[1].Op())
	}
	if recs[2].Op() != tx.SETINT {
		t.Errorf("Expected SETINT, got %d", recs[2].Op())
	}
	if recs[3].Op() != tx.START {
		t.Errorf("Expected START, got %d", recs[3].Op())
	}
}
