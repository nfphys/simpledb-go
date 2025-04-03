package main

import (
	"os"

	"github.com/nfphys/simpledb-go/file"
	"github.com/nfphys/simpledb-go/log"
)

func main() {
	dbDir := "testdb"
	blocksize := 32

	fm := file.NewFileMgr(dbDir, blocksize)
	defer fm.Close()

	logfile := "logfile"
	lm := log.NewLogMgr(fm, logfile)

	lm.Append([]byte("record1"))
	lm.Append([]byte("record2"))
	lm.Append([]byte("record3"))
	lm.Append([]byte("record4"))
	lm.Append([]byte("record5"))

	for rec := range lm.Iterator() {
		println(string(rec))
	}

	os.RemoveAll("testdb")
}
