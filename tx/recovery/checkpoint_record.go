package recovery

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
)

type CheckpointRecord struct {
}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (cr *CheckpointRecord) Op() int {
	return CHECKPOINT
}

func (cr *CheckpointRecord) TxNumber() int {
	return -1
}

func (cr *CheckpointRecord) Undo(tx *tx.Transaction) {
	// No undo operation for CHECKPOINT record
}

func (cr *CheckpointRecord) ToString() string {
	return fmt.Sprintf("<CHECKPOINT %d>", cr.TxNumber())
}

func WriteCheckpointRecordToLog(lm *log.LogMgr, txnum int) int {
	rec := make([]byte, 4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}
