package log

import (
	"sync"

	"github.com/nfphys/simpledb-go/file"
)

type LogMgr struct {
	fm *file.FileMgr
	logfile string
	logpage *file.Page // layout: [boundary(uint32)]...[rec3][rec2][rec1]
	currentblk *file.BlockId
	latestLSN int
	lastSavedLSN int
	mu sync.Mutex
}

func NewLogMgr(fm *file.FileMgr, logfile string) *LogMgr {
	logpage := file.NewPage(fm.BlockSize())

	var currentblk *file.BlockId
	if logsize := fm.Length(logfile); logsize == 0 {
		currentblk = fm.Append(logfile)
		logpage.SetInt(0, fm.BlockSize())
		fm.Write(currentblk, logpage)
	} else {
		currentblk = file.NewBlockId(logfile, logsize-1)
		fm.Read(currentblk, logpage)
	}

	return &LogMgr{
		fm: fm,
		logfile: logfile,
		logpage: logpage,
		currentblk: currentblk,
		latestLSN: 0,
		lastSavedLSN: 0,
		mu: sync.Mutex{},
	}
}

func (lm *LogMgr) Append(rec []byte) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logpage.GetInt(0)
	recsize := len(rec)
	bytesneeded := recsize + file.INT_BYTES

	if boundary - bytesneeded < file.INT_BYTES {
		lm.flush()
		lm.currentblk = lm.fm.Append(lm.logfile)
		lm.logpage.SetInt(0, lm.fm.BlockSize())
		lm.fm.Write(lm.currentblk, lm.logpage)
		boundary = lm.logpage.GetInt(0)
	}

	recpos := boundary - bytesneeded

	lm.logpage.SetBytes(recpos, rec)
	lm.logpage.SetInt(0, recpos)
	lm.latestLSN += 1

	return lm.latestLSN
}

func (lm *LogMgr) Flush(lsn int) {
	if lsn <= lm.lastSavedLSN {
		return
	}

	lm.fm.Write(lm.currentblk, lm.logpage)
}

func (lm *LogMgr) Iterator() func(func([]byte) bool) {
	lm.flush()

	return func(yield func([]byte) bool) {
		p := file.NewPage(lm.fm.BlockSize())

		blk := lm.currentblk
		lm.fm.Read(blk, p)
		currentpos := p.GetInt(0)

		for {
			if currentpos == lm.fm.BlockSize() && blk.Number() == 0 {
				break
			}

			if currentpos == lm.fm.BlockSize() {
				blk = file.NewBlockId(lm.logfile, blk.Number()-1)
				lm.fm.Read(blk, p)
				currentpos = p.GetInt(0)
			}

			rec := p.GetBytes(currentpos)
			currentpos += len(rec) + file.INT_BYTES

			if !yield(rec) {
				return
			}
		}
	}
}

func (lm *LogMgr) flush() {
	lm.fm.Write(lm.currentblk, lm.logpage)
	lm.lastSavedLSN = lm.latestLSN
}
