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

	return &Transaction{
		fm: fm,
		lm: lm,
		bm: bm,
		txnum: txnum,
		mybuffers: NewBufferList(bm),
	}
}

func (tx *Transaction) Commit() {
	// TODO: implement recovery and concurrency control
	tx.mybuffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	// TODO: implement recovery and concurrency control
	tx.mybuffers.UnpinAll()
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
	// TODO: implement recovery and concurrency control
	tx.setIntWithoutLog(blk, offset, val)
}

func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string) {
	// TODO: implement recovery and concurrency control
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
