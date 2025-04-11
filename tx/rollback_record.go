package tx

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

type RollbackRecord struct {
	txnum int
}

func NewRollbackRecord(p *file.Page) *RollbackRecord {
	return &RollbackRecord{
		txnum: p.GetInt(4),
	}
}

func (rr *RollbackRecord) Op() int {
	return ROLLBACK
}

func (rr *RollbackRecord) TxNumber() int {
	return rr.txnum
}

func (rr *RollbackRecord) Undo(tx *Transaction) {
	// No undo operation for ROLLBACK record
}

func (rr *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", rr.txnum)
}

func WriteRollbackRecordToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, ROLLBACK)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
