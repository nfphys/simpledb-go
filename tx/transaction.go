package tx

import (
	"sync"

	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
	"github.com/nfphys/simpledb-go/tx/recovery"
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
	rm *recovery.RecoveryMgr
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	mu.Lock()
	defer mu.Unlock()

	txnum := nextTxNum
	nextTxNum++

	tx := &Transaction{
		fm: fm,
		lm: lm,
		bm: bm,
		txnum: txnum,
		mybuffers: NewBufferList(bm),
	}

	rm := recovery.NewRecoveryMgr(tx, lm, bm)
	tx.rm = rm

	return tx
}

func (tx *Transaction) TxNumber() int {
	return tx.txnum
}

func (tx *Transaction) Commit() {
	// TODO: implement concurrency control
	tx.rm.Commit()
	tx.mybuffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	// TODO: implement concurrency control
	tx.rm.Rollback()
	tx.mybuffers.UnpinAll()
}

func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.rm.Recover()
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

func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int, okToLog bool) {
	// TODO: implement concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)

	lsn := -1
	if (okToLog) {
		lsn = tx.rm.SetInt(buffer, offset, val)
	}

	buffer.Contents().SetInt(offset, val)
	buffer.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string, okToLog bool) {
	// TODO: implement concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)

	lsn := -1
	if (okToLog) {
		lsn = tx.rm.SetString(buffer, offset, val)
	}

	buffer.Contents().SetString(offset, val)
	buffer.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) AvailableBuffs() int {
	return tx.bm.Available()
}

func (tx *Transaction) Size(filename string) int {
	// TODO: implement concurrency control
	return tx.fm.Length(filename)
}

func (tx *Transaction) Append(filename string) *file.BlockId {
	// TODO: implement concurrency control
	return tx.fm.Append(filename)
}

func (tx *Transaction) BlockSize() int {
	return tx.fm.BlockSize()
}
