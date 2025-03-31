package file

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type FileMgr struct {
	dbDir string
	blocksize int
	openFiles map[string]*os.File
	mu sync.Mutex
}

func NewFileMgr(dbDir string, blocksize int) *FileMgr {
	err := os.MkdirAll(dbDir, 0777)
	if err != nil && !os.IsExist(err) {
		panic(err)
	}

	return &FileMgr{
		dbDir: dbDir,
		blocksize: blocksize,
		openFiles: make(map[string]*os.File),
		mu: sync.Mutex{},
	}
}

func (fm *FileMgr) Read(blk *BlockId, p *Page) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(blk.FileName())
	if err != nil {
		panic(err)
	}

	b := make([]byte, fm.blocksize)
	file.Seek(int64(blk.Number()*fm.blocksize), io.SeekStart)
	file.Read(b)

	copy(p.contents(), b)
}

func (fm *FileMgr) Write(blk *BlockId, p *Page) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(blk.FileName())
	if err != nil {
		panic(err)
	}

	file.Seek(int64(blk.Number()*fm.blocksize), io.SeekStart)
	file.Write(p.contents())
}

func (fm *FileMgr) Append(filename string) *BlockId {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	return NewBlockId(filename, fm.Length(filename))
}

func (fm *FileMgr) Length(filename string) int {
	file, err := fm.getFile(filename)
	if err != nil {
		panic(err)
	}

	bytes, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	
	return int(bytes) / fm.BlockSize()
}

func (fm *FileMgr) BlockSize() int {
	return fm.blocksize
}

func (fm *FileMgr) Close() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, file := range fm.openFiles {
		file.Close()
	}
}

func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	file, ok := fm.openFiles[filename]
	if ok {
		return file, nil
	}
	
	file, err := os.OpenFile(fmt.Sprintf("%s/%s", fm.dbDir, filename), os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	fm.openFiles[filename] = file
	return file, nil
}
