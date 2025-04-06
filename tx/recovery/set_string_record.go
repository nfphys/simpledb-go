package recovery

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

type SetStringRecord struct {
	txnum int
	offset int
	val string
	blk *file.BlockId
}

func NewSetStringRecord(p *file.Page) *SetStringRecord {
	tpos := 4
	txnum := p.GetInt(4)

	fpos := tpos + 4
	filename := p.GetString(8)

	bpos := fpos + 4 + len(filename)
	blknum := p.GetInt(bpos)
	blk := file.NewBlockId(filename, blknum)

	opos := bpos + 4
	offset := p.GetInt(opos)

	vpos := opos + 4
	val := p.GetString(vpos)

	return &SetStringRecord{
		txnum: txnum,
		offset: offset,
		val: val,
		blk: blk,
	}
}

func (sir *SetStringRecord) Op() int {
	return SETSTRING
}

func (sir *SetStringRecord) TxNumber() int {
	return sir.txnum
}

func (sir *SetStringRecord) Undo(tx Transaction) {
	tx.Pin(sir.blk)
	tx.SetString(sir.blk, sir.offset, sir.val, false)
	tx.Unpin(sir.blk)
}

func (sir *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", sir.txnum, sir.blk.FileName(), sir.blk.Number(), sir.val)
}

func WriteSetStringRecordToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, val string) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + 4 + len(blk.FileName())
	opos := bpos + 4
	vpos := opos + 4

	rec := make([]byte, vpos + 4 + len(val))
	p := file.NewPageFromBytes(rec)

	p.SetInt(0, SETSTRING)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return lm.Append(rec)
}
