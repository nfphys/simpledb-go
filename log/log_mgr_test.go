package log_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

func TestLogMgr(t *testing.T) {
	testPath := filepath.Join(os.TempDir(), "testdb")
	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)

	blocksize := 32
	fm := file.NewFileMgr(testPath, blocksize)
	defer fm.Close()

	logfile := "logfile"
	lm := log.NewLogMgr(fm, logfile)

	if lsn := lm.Append([]byte("record1")); lsn != 1 {
		t.Errorf("Expected 1, got %d", lsn)
	}
	if lsn := lm.Append([]byte("record2")); lsn != 2 {
		t.Errorf("Expected 2, got %d", lsn)
	}
	if lsn := lm.Append([]byte("record3")); lsn != 3 {
		t.Errorf("Expected 3, got %d", lsn)
	}
	if lsn := lm.Append([]byte("record4")); lsn != 4 {
		t.Errorf("Expected 4, got %d", lsn)
	}

	lm2 := log.NewLogMgr(fm, logfile)
	num := 2
	for rec := range lm2.Iterator() {
		expected := fmt.Sprintf("record%d", num)
		if string(rec) != expected {
			t.Errorf("Expected %s, got %s", expected, string(rec))
		}
		num--
	}

	lm.Flush(4)

	lm3 := log.NewLogMgr(fm, logfile)
	num = 4
	for rec := range lm3.Iterator() {
		expected := fmt.Sprintf("record%d", num)
		if string(rec) != expected {
			t.Errorf("Expected %s, got %s", expected, string(rec))
		}
		num--
	}
}
