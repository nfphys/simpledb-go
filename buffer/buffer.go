package buffer

import (
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

type Buffer struct {
	fm *file.FileMgr
	lm *log.LogMgr
	contents *file.Page
	blk *file.BlockId
	pins int
	modified bool
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm: fm,
		lm: lm,
		contents: file.NewPage(fm.BlockSize()),
		blk: nil,
		pins: 0,
		modified: false,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockId {
	return b.blk
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) SetModified() {
	b.modified = true
}

func (b *Buffer) Flush() {
	if b.modified {
		b.fm.Write(b.blk, b.contents)
		b.modified = false
	}
}

func (b *Buffer) assignToBlock(blk *file.BlockId) {
	b.Flush()
	b.blk = blk
	b.fm.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
