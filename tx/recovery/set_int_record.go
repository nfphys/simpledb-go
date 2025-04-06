package recovery

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
)

type SetIntRecord struct {
	txnum int
	offset int
	val int
	blk *file.BlockId
}

func NewSetIntRecord(p *file.Page) *SetIntRecord {
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
	val := p.GetInt(vpos)

	return &SetIntRecord{
		txnum: txnum,
		offset: offset,
		val: val,
		blk: blk,
	}
}

func (sir *SetIntRecord) Op() int {
	return SETINT
}

func (sir *SetIntRecord) TxNumber() int {
	return sir.txnum
}

func (sir *SetIntRecord) Undo(tx *tx.Transaction) {
	tx.Pin(sir.blk)
	tx.SetInt(sir.blk, sir.offset, sir.val, false)
	tx.Unpin(sir.blk)
}

func (sir *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", sir.txnum, sir.blk.FileName(), sir.blk.Number(), sir.offset)
}

func WriteSetIntRecordToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, val int) int {
	tpos := 4
	fpos := tpos + 4
	bpos := fpos + 4 + len(blk.FileName())
	opos := bpos + 4
	vpos := opos + 4

	rec := make([]byte, vpos+4)
	p := file.NewPageFromBytes(rec)

	p.SetInt(0, SETINT)
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return lm.Append(rec)
}
