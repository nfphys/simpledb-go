package recovery

import (
	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

type RecoveryMgr struct {
	tx Transaction
	txnum int
	lm *log.LogMgr
	bm *buffer.BufferMgr
}

type Transaction interface {
	TxNumber() int
	Pin(*file.BlockId) error
	Unpin(*file.BlockId)
	GetInt(*file.BlockId, int) int
	GetString(*file.BlockId, int) string
	SetInt(*file.BlockId, int, int, bool)
	SetString(*file.BlockId, int, string, bool)
}

func NewRecoveryMgr(tx Transaction, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	WriteStartRecordToLog(lm, tx.TxNumber())

	return &RecoveryMgr{
		tx: tx,
		txnum: tx.TxNumber(),	
		lm: lm,
		bm: bm,
	}
}

func (rm *RecoveryMgr) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCommitRecordToLog(rm.lm, rm.tx.TxNumber())
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteRollbackRecordToLog(rm.lm, rm.tx.TxNumber())
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := WriteCheckpointRecordToLog(rm.lm)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset int, newval int) int {
	blk := buff.Block()
	oldval := buff.Contents().GetInt(offset)
	return WriteSetIntRecordToLog(rm.lm, rm.txnum, blk, offset, oldval)
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, newval string) int {
	blk := buff.Block()
	oldval := buff.Contents().GetString(offset)
	return WriteSetStringRecordToLog(rm.lm, rm.txnum, blk, offset, oldval)
}

func (rm *RecoveryMgr) doRollback() {
	for bytes := range rm.lm.Iterator() {
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() != rm.txnum {
			continue
		}

		if rec.Op() == START {
			return
		}

		rec.Undo(rm.tx)
	}
}

func (rm *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int]bool)
	for bytes := range rm.lm.Iterator() {
		rec := CreateLogRecord(bytes)
		switch rec.Op() {
		case CHECKPOINT:
			return
		case COMMIT, ROLLBACK:
			finishedTxs[rec.TxNumber()] = true
		default:
			if !finishedTxs[rec.TxNumber()] {
				rec.Undo(rm.tx)
			}
		}
	}
}
