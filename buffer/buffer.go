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
	txnum int
	lsn int
}

func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm: fm,
		lm: lm,
		contents: file.NewPage(fm.BlockSize()),
		blk: nil,
		pins: 0,
		txnum: -1,
		lsn: -1,
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

func (b *Buffer) SetModified(txnum int, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) ModifiyingTx() int {
	return b.txnum
}

func (b *Buffer) assignToBlock(blk *file.BlockId) {
	b.flush()
	b.blk = blk
	b.fm.Read(blk, b.contents)
	b.pins = 0
}

func (b *Buffer) flush() {
	if (b.txnum >= 0) {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.blk, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
