package recovery

import (
	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx"
)

type RecoveryMgr struct {
	tx *tx.Transaction
	lm *log.LogMgr
	bm *buffer.BufferMgr
}

func NewRecoveryMgr(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	tx := tx.NewTransaction(fm, lm, bm)

	return &RecoveryMgr{
		tx: tx,
		lm: lm,
		bm: bm,
	}
}

func (rm *RecoveryMgr) Recover() {
	rm.doRecover()
	lsn := tx.WriteCheckpointRecordToLog(rm.lm)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int]bool)
	for bytes := range rm.lm.Iterator() {
		rec := tx.CreateLogRecord(bytes)
		switch rec.Op() {
		case tx.CHECKPOINT:
			return
		case tx.COMMIT, tx.ROLLBACK:
			finishedTxs[rec.TxNumber()] = true
		default:
			if !finishedTxs[rec.TxNumber()] {
				rec.Undo(rm.tx)
			}
		}
	}
}
