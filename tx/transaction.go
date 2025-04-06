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

func (tx *Transaction) Recover() {
	// TODO: implement recovery
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
	// TODO: implement recovery and concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)
	buffer.Contents().SetInt(offset, val)
	buffer.SetModified(tx.txnum, -1)
}

func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string, okToLog bool) {
	// TODO: implement recovery and concurrency control
	buffer := tx.mybuffers.GetBuffer(blk)
	buffer.Contents().SetString(offset, val)
	buffer.SetModified(tx.txnum, -1)
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
