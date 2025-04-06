package recovery

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
)

type CommitRecord struct {
	txnum int
}

func NewCommitRecord(p *file.Page) *CommitRecord {
	return &CommitRecord{
		txnum: p.GetInt(4),
	}
}

func (cr *CommitRecord) Op() int {
	return COMMIT
}

func (cr *CommitRecord) TxNumber() int {
	return cr.txnum
}

func (cr *CommitRecord) Undo(tx *tx.Transaction) {
	// No undo operation for COMMIT record
}

func (cr *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", cr.txnum)
}

func WriteCommitRecordToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 8)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(4, txnum)
	return lm.Append(rec)
}
