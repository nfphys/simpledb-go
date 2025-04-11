package file

import (
	"encoding/binary"
)

const (
	INT_BYTES = 4
)

type Page struct {
	b []byte
}

func NewPage(blocksize int) *Page {
	return &Page{
		b: make([]byte, blocksize),
	}
}

func NewPageFromBytes(b []byte) *Page {
	return &Page{
		b: b,
	}
}

func (p *Page) GetInt(offset int) int {
	return int(binary.LittleEndian.Uint32(p.b[offset:offset+INT_BYTES]))
}

func (p *Page) SetInt(offset int, n int) {
	binary.LittleEndian.PutUint32(p.b[offset:offset+INT_BYTES], uint32(n))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset) // byte列は、先頭4バイトに長さが格納されている
	return p.b[offset+INT_BYTES : offset+INT_BYTES+length]
}

func (p *Page) SetBytes(offset int, b []byte) {
	p.SetInt(offset, len(b)) // byte列は、先頭4バイトに長さを格納する
	copy(p.b[offset+INT_BYTES:], b)
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetString(offset int, s string) {
	p.SetBytes(offset, []byte(s))
}

// FileMgr用のprivateメソッド
func (p *Page) contents() []byte {
	return p.b
}
