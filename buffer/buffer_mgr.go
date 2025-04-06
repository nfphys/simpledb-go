package buffer

import (
	"errors"
	"sync"
	"time"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

const (
	MAX_TIME = 1000 // 1 seconds
)

var (
	ErrBufferNotFound = errors.New("buffer not found")
)

type BufferMgr struct {
	fm *file.FileMgr
	lm *log.LogMgr
	bufferpool []*Buffer
	numAvailable int
	mu *sync.Mutex
	cond *sync.Cond
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int) *BufferMgr {
	bufferpool := make([]*Buffer, numbuffs)
	for i := 0; i < numbuffs; i++ {
		bufferpool[i] = NewBuffer(fm, lm)
	}

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)

	return &BufferMgr{
		fm: fm,
		lm: lm,
		bufferpool: bufferpool,
		numAvailable: numbuffs,
		mu: &mu,
		cond: cond,
	}
}

func (bm *BufferMgr) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	return bm.numAvailable
}

func (bm *BufferMgr) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buff := range bm.bufferpool {
		if buff.ModifiyingTx() == txnum {
			buff.flush()
		}
	}
}

func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff.unpin()
	if (!buff.IsPinned()) {
		bm.numAvailable++
		bm.cond.Broadcast() // signal all waiting threads
	}
}

func (bm *BufferMgr) Pin(blk *file.BlockId) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buff := bm.tryToPin(blk)
	if buff != nil {
		return buff, nil
	}

	timeout := time.After(MAX_TIME * time.Millisecond)
	go func() {
		<-timeout
		bm.cond.Broadcast() // signal all waiting threads
	}()
	
	start := time.Now()
	for buff == nil && time.Since(start) < MAX_TIME*time.Millisecond {
		bm.cond.Wait() // release the lock and wait for a signal
		buff = bm.tryToPin(blk)
	}

	if buff == nil {
		return nil, ErrBufferNotFound
	}

	return buff, nil
}

func (bm *BufferMgr) tryToPin(blk *file.BlockId) *Buffer {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil
		}
		buff.assignToBlock(blk)
	}

	if !buff.IsPinned() {
		bm.numAvailable--
	}

	buff.pin()
	return buff
}

func (bm *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, buff := range bm.bufferpool {
		blk2 := buff.Block()
		if blk2 != nil && blk2.Equals(blk) {
			return buff
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	for _, buff := range bm.bufferpool {
		if !buff.IsPinned() {
			return buff
		}
	}
	return nil
}
