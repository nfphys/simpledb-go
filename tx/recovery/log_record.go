package recovery

import (
	"github.com/nfphys/simpledb-go/file"
)

const (
	CHECKPOINT = 0
	START = 1
	COMMIT = 2
	ROLLBACK = 3
	SETINT = 4
	SETSTRING = 5
)

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx Transaction)
	ToString() string
}

func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	op := p.GetInt(0)
	switch op {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("Unknown log record type")
	}
}
