package main

import (
	"bytes"
	"encoding/binary"
)

type byteItem []byte

func newByteItem(k, v []byte) byteItem {
	b := make([]byte, 2, 2+len(k)+len(v))
	binary.LittleEndian.PutUint16(b[0:2], uint16(len(k)))
	b = append(b, k...)
	b = append(b, v...)

	return byteItem(b)
}

func (b *byteItem) valOffset() int {
	buf := []byte(*b)
	l := binary.LittleEndian.Uint16(buf[0:2])
	return 2 + int(l)
}

func (b *byteItem) Key() []byte {
	buf := []byte(*b)
	return buf[2:b.valOffset()]
}

func (b *byteItem) Value() []byte {
	buf := []byte(*b)
	return buf[b.valOffset():]
}

func byteItemKeyCompare(a, b []byte) int {
	var l int
	itm1 := byteItem(a)
	itm2 := byteItem(b)

	k1 := []byte(itm1)[2:itm1.valOffset()]
	k2 := []byte(itm2)[2:itm2.valOffset()]

	if len(k1) > len(k2) {
		l = len(k2)
	} else {
		l = len(k1)
	}

	return bytes.Compare(k1[:l], k2[:l])
}
