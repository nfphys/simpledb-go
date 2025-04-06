package recovery

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
)

type StartRecord struct {
	txnum int
}

func NewStartRecord(p *file.Page) *StartRecord {
	return &StartRecord{
		txnum: p.GetInt(4),
	}
}

func (sr *StartRecord) Op() int {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txnum
}

func (sr *StartRecord) Undo(tx *tx.Transaction) {
	// No undo operation for START record
}

func (sr *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", sr.txnum)
}

func WriteStartRecordToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, START)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
