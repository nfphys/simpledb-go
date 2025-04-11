package tx

import (
	"fmt"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
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

func (cr *CheckpointRecord) Undo(tx *Transaction) {
	// No undo operation for CHECKPOINT record
}

func (cr *CheckpointRecord) ToString() string {
	return fmt.Sprintf("<CHECKPOINT %d>", cr.TxNumber())
}

func WriteCheckpointRecordToLog(lm *log.LogMgr) int {
	rec := make([]byte, 4)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}
