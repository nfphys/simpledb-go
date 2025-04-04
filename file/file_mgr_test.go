package file_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nfphys/simpledb-go/file"
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

func TestWriteRead(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	blk := file.NewBlockId("testfile", 0)
	p1 := file.NewPage(blocksize)
	p2 := file.NewPage(blocksize)

	p1.SetString(0, "Hello, World!")
	p1.SetInt(100, 123)

	// When
	fm.Write(blk, p1)
	fm.Read(blk, p2)

	// Then
	if readStr := p2.GetString(0); readStr != "Hello, World!" {
		t.Errorf("Expected string 'Hello, World!', got '%s'", readStr)
	}
	if readInt := p2.GetInt(100); readInt != 123 {
		t.Errorf("Expected int 123, got %d", readInt)
	}
}

func TestAppend(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	blk := file.NewBlockId("testfile", 3)
	p := file.NewPage(blocksize)
	fm.Write(blk, p)

	// When
	blk2 := fm.Append("testfile")

	// Then
	if blk2.FileName() != "testfile" {
		t.Errorf("Expected filename 'testfile', got '%s'", blk2.FileName())
	}
	if blk2.Number() != 4 {
		t.Errorf("Expected block number 4, got %d", blk2.Number())
	}
}

func TestLength(t *testing.T) {
	// Given
	blocksize := 4096
	fm := setup(blocksize)
	defer cleanup(fm)

	blk := file.NewBlockId("testfile", 3)
	p := file.NewPage(blocksize)
	fm.Write(blk, p)

	// When
	length := fm.Length("testfile")

	// Then
	if length != 4 {
		t.Errorf("Expected length 4, got %d", length)
	}
}
