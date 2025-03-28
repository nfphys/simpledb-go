package main

import (
	"fmt"
	"os"

	"github.com/nfphys/simpledb-go/file"
)

func main() {
	dbDir := "testdb"
	blocksize := 2048

	fm := file.NewFileMgr(dbDir, blocksize)
	defer fm.Close()

	blk1 := file.NewBlockId("testfile", 0)
	blk2 := file.NewBlockId("testfile", 1)

	p1 := file.NewPage(blocksize)
	p2 := file.NewPage(blocksize)

	p1.SetString(0, "Hello, world!")
	p1.SetInt(100, 12345)
	p2.SetString(0, "Goodbye, world!")
	p2.SetInt(100, 67890)

	fm.Write(blk1, p1)
	fm.Write(blk2, p2)

	p3 := file.NewPage(blocksize)
	p4 := file.NewPage(blocksize)

	fm.Read(blk1, p3)
	fm.Read(blk2, p4)

	fmt.Println(p3.GetString(0))
	fmt.Println(p3.GetInt(100))
	fmt.Println(p4.GetString(0))
	fmt.Println(p4.GetInt(100))

	os.RemoveAll("testdb")
}
