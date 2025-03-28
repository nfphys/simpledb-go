package file

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	testDir     = "testdb"
	testFile    = "testfile"
	blockSize   = 400
	testStr     = "abcdefghijklmnopqrstuvwxyz"
	testInt     = 101
)

func setupTest() *FileMgr {
	testPath := filepath.Join(os.TempDir(), testDir)
	os.RemoveAll(testPath)
	fm := NewFileMgr(testPath, blockSize)
	return fm
}

func cleanupTest() {
	testPath := filepath.Join(os.TempDir(), testDir)
	os.RemoveAll(testPath)
}

func TestFileMgrBasic(t *testing.T) {
	fm := setupTest()
	defer cleanupTest()
	
	blk := NewBlockId(testFile, 0)
	p1 := NewPage(blockSize)
	p1.SetString(0, testStr)
	p1.SetInt(100, testInt)
	fm.Write(blk, p1)
	
	p2 := NewPage(blockSize)
	fm.Read(blk, p2)
	
	readStr := p2.GetString(0)
	readInt := p2.GetInt(100)
	
	if readStr != testStr {
		t.Errorf("String read mismatch. Expected: %s, Got: %s", testStr, readStr)
	}
	
	if readInt != testInt {
		t.Errorf("Int read mismatch. Expected: %d, Got: %d", testInt, readInt)
	}
	
	fm.Close()
}

func TestMultipleBlocks(t *testing.T) {
	fm := setupTest()
	defer cleanupTest()
	
	blk0 := NewBlockId(testFile, 0)
	blk1 := NewBlockId(testFile, 1)
	blk2 := NewBlockId(testFile, 2)
	
	pages := make([]*Page, 3)
	values := []int{10, 20, 30}
	
	for i := 0; i < 3; i++ {
		pages[i] = NewPage(blockSize)
		pages[i].SetInt(0, values[i])
	}
	
	fm.Write(blk0, pages[0])
	fm.Write(blk1, pages[1])
	fm.Write(blk2, pages[2])
	
	for i := 0; i < 3; i++ {
		p := NewPage(blockSize)
		blk := NewBlockId(testFile, i)
		fm.Read(blk, p)
		
		if p.GetInt(0) != values[i] {
			t.Errorf("Block %d: value mismatch. Expected: %d, Got: %d", i, values[i], p.GetInt(0))
		}
	}
	
	fm.Close()
}

func TestAppend(t *testing.T) {
	fm := setupTest()
	defer cleanupTest()
	
	blk1 := fm.Append(testFile)
	if blk1.Number() != 0 {
		t.Errorf("First block should have number 0, got %d", blk1.Number())
	}
	
	p1 := NewPage(blockSize)
	p1.SetInt(0, 999)
	fm.Write(blk1, p1)
	
	blk2 := fm.Append(testFile)
	if blk2.Number() != 1 {
		t.Errorf("Second block should have number 1, got %d", blk2.Number())
	}
	
	p2 := NewPage(blockSize)
	fm.Read(blk1, p2)
	if p2.GetInt(0) != 999 {
		t.Errorf("Data in first block corrupted. Expected: 999, Got: %d", p2.GetInt(0))
	}
	
	fm.Close()
}
