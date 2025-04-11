package tx

import (
	"sync"

	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

var (
	nextTxNum int = 0
	mu sync.Mutex
)

type Transaction struct {
	fm *file.FileMgr
	lm *log.LogMgr
	bm *buffer.BufferMgr
	txnum int
	mybuffers *BufferList
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	mu.Lock()
	defer mu.Unlock()

	txnum := nextTxNum
	nextTxNum++

	WriteStartRecordToLog(lm, txnum)

	return &Transaction{
		fm: fm,
		lm: lm,
		bm: bm,
		txnum: txnum,
		mybuffers: NewBufferList(bm),
	}
}

func (tx *Transaction) Commit() {
	// TODO: implement concurrency control
	tx.mybuffers.FlushAll()
	lsn := WriteCommitRecordToLog(tx.lm, tx.txnum)
	tx.lm.Flush(lsn)
	tx.mybuffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	// TODO: implement concurrency control
	tx.doRollback()
	WriteRollbackRecordToLog(tx.lm, tx.txnum)
	tx.mybuffers.UnpinAll()
}

func (tx *Transaction) doRollback() {
	for bytes := range tx.lm.Iterator() {
		rec := CreateLogRecord(bytes)
		if rec.TxNumber() != tx.txnum {
			continue
		}

		if rec.Op() == START {
			break
		}

		rec.Undo(tx)
	}
}

func (tx *Transaction) Pin(blk *file.BlockId) error {
	return tx.mybuffers.Pin(blk)
}

func (tx *Transaction) Unpin(blk *file.BlockId) {
	tx.mybuffers.Unpin(blk)
}

func (tx *Transaction) GetInt(blk *file.BlockId, offset int) int {
	// TODO: implement concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)

	return buffer.Contents().GetInt(offset)
}

func (tx *Transaction) GetString(blk *file.BlockId, offset int) string {
	// TODO: implement concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)

	return buffer.Contents().GetString(offset)
}

func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int) {
	// TODO: implement concurrency control
	oldval := tx.GetInt(blk, offset)
	WriteSetIntRecordToLog(tx.lm, tx.txnum, blk, offset, oldval)
	tx.setIntWithoutLog(blk, offset, val)
}

func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string) {
	// TODO: implement concurrency control
	oldval := tx.GetString(blk, offset)
	WriteSetStringRecordToLog(tx.lm, tx.txnum, blk, offset, oldval)
	tx.setStringWithoutLog(blk, offset, val)
}

func (tx *Transaction) setIntWithoutLog(blk *file.BlockId, offset int, val int) {
	buffer := tx.mybuffers.GetBuffer(blk)
	buffer.Contents().SetInt(offset, val)
	buffer.SetModified()
}

func (tx *Transaction) setStringWithoutLog(blk *file.BlockId, offset int, val string) {
	buffer := tx.mybuffers.GetBuffer(blk)
	buffer.Contents().SetString(offset, val)
	buffer.SetModified()
}
