package tx

import (
	"github.com/nfphys/simpledb-go/buffer"
	"github.com/nfphys/simpledb-go/file"
)

type BufferList struct {
	buffers map[file.BlockId]*buffer.Buffer
	pins map[file.BlockId]int
	bm *buffer.BufferMgr
}

func NewBufferList(bm *buffer.BufferMgr) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockId]*buffer.Buffer),
		pins: make(map[file.BlockId]int),
		bm: bm,
	}
}

func (bl *BufferList) GetBuffer(blk *file.BlockId) *buffer.Buffer {
	buff := bl.buffers[*blk]
	return buff
}

func (bl *BufferList) Pin(blk *file.BlockId) error {
	buff, err := bl.bm.Pin(blk)
	if err != nil {
		return err
	}

	bl.buffers[*blk] = buff
	bl.pins[*blk]++
	return nil
}

func (bl *BufferList) Unpin(blk *file.BlockId) {
	buff, ok := bl.buffers[*blk]
	if !ok {
		return 
	}

	bl.bm.Unpin(buff)
	bl.pins[*blk]--
	if bl.pins[*blk] == 0 {
		delete(bl.buffers, *blk)
	}
}

func (bl *BufferList) UnpinAll() {
	for blk := range bl.buffers {
		buff := bl.buffers[blk]
		bl.bm.Unpin(buff)
	}
	bl.buffers = make(map[file.BlockId]*buffer.Buffer)
	bl.pins = make(map[file.BlockId]int)
}
