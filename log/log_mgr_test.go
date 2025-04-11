package log_test

import (
	"os"
	"path/filepath"
	"testing"

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

func TestAppend(t *testing.T) {
	// Given
	blocksize := 32
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")

	// When
	lsn1 := lm.Append([]byte("record1"))
	lsn2 := lm.Append([]byte("record2"))
	lsn3 := lm.Append([]byte("record3"))
	lm.Flush(lsn3)

	// Then
	blk1 := file.NewBlockId("logfile", 0)
	blk2 := file.NewBlockId("logfile", 1)
	p1 := file.NewPage(blocksize)
	p2 := file.NewPage(blocksize)
	fm.Read(blk1, p1)
	fm.Read(blk2, p2)

	if lsn1 != 1 {
		t.Errorf("Expected 1, got %d", lsn1)
	}
	if lsn2 != 2 {
		t.Errorf("Expected 2, got %d", lsn2)
	}
	if lsn3 != 3 {
		t.Errorf("Expected 3, got %d", lsn3)
	}
	if p1.GetInt(0) != 10 {
		t.Errorf("Expected boundary %d, got %d", 10, p1.GetInt(0))
	}
	if p2.GetInt(0) != 21 {
		t.Errorf("Expected boundary %d, got %d", 21, p2.GetInt(0))
	}
	if p1.GetString(21) != "record1" {
		t.Errorf("Expected 'record1', got '%s'", p1.GetString(21))
	}
	if p1.GetString(10) != "record2" {
		t.Errorf("Expected 'record2', got '%s'", p1.GetString(10))
	}
	if p2.GetString(21) != "record3" {
		t.Errorf("Expected 'record3', got '%s'", p2.GetString(21))
	}
}

func TestIterator(t *testing.T) {
	// Given
	blocksize := 32
	fm := setup(blocksize)
	defer cleanup(fm)

	lm := log.NewLogMgr(fm, "logfile")

	lm.Append([]byte("record1"))
	lm.Append([]byte("record2"))
	lm.Append([]byte("record3"))
	lm.Flush(3)

	// When
	logs := []string{}
	for rec := range lm.Iterator() {
		logs = append(logs, string(rec))
	}

	// Then
	if len(logs) != 3 {
		t.Errorf("Expected 3 records, got %d", len(logs))
	}
	if logs[0] != "record3" {
		t.Errorf("Expected 'record3', got '%s'", logs[0])
	}
	if logs[1] != "record2" {
		t.Errorf("Expected 'record2', got '%s'", logs[1])
	}
	if logs[2] != "record1" {
		t.Errorf("Expected 'record1', got '%s'", logs[2])
	}
}
