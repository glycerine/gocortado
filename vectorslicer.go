package gocortado

import (
	"fmt"
)

type VectorSlicer[T any] struct {
	vec   []T
	chunk int
	beg   int
	endx  int
	done  bool
	name  string
}

func NewVectorSlicer[T any](vec []T, beg, length, chunk int, name string) (r *VectorSlicer[T]) {
	if beg < 0 {
		panic("beg must be >= 0")
	}
	if beg >= len(vec) {
		panic(fmt.Sprintf("beg(%v) must be < len(vec)=%v", beg, len(vec)))
	}
	endx := beg + length
	if len(vec) < endx {
		endx = len(vec)
	}
	r = &VectorSlicer[T]{
		vec:   vec,
		chunk: chunk,
		beg:   beg,
		endx:  endx,
		name:  name,
	}
	return
}

// Fetch returns the current slice of vs.vec in r, without advancing.
// If there were any available elements, done will return false.
// Otherwise, when done comes back true, r will be length 0.
// So always check the done flag before using r.
func (vs *VectorSlicer[T]) Fetch() (r []T, done bool) {
	if vs.done {
		done = true
		return
	}
	if vs.beg >= vs.endx {
		done = true
		vs.done = true
		return
	}
	e := vs.beg + vs.chunk
	if e >= vs.endx {
		e = vs.endx
	}
	r = vs.vec[vs.beg:e]
	return
}

// Adv advances the iterator by at least chunk. If there were
// any available rows, done will be false. If there
// were no more available rows, done will return true.
func (vs *VectorSlicer[T]) Adv() (done bool) {
	if vs.done {
		done = true
		return
	}
	if vs.beg >= vs.endx {
		done = true
		vs.done = true
		return
	}
	vs.beg += vs.chunk
	if vs.beg >= vs.endx {
		vs.beg = vs.endx
	}
	return
}

// FetchAdv returns current slice (of length <= chunk);
// and then advances beg by chunk (or less if we run out of elements).
// If done returns true, because we ran out of elements on the
// last Fetch() or FetchAdv() call, then the r returned will be length 0.
func (vs *VectorSlicer[T]) FetchAdv() (r []T, done bool) {
	if vs.done {
		done = true
		return
	}
	if vs.beg >= vs.endx {
		done = true
		vs.done = true
		return
	}
	e := vs.beg + vs.chunk
	if e >= vs.endx {
		e = vs.endx
	}
	//vv("FetchAdv('%v') returning slice [%v, %v)", vs.name, vs.beg, e)
	r = vs.vec[vs.beg:e]
	vs.beg = e
	return
}

// FetchAdv1 has one return, no done. Otherwise same as FetchAdv.
func (vs *VectorSlicer[T]) FetchAdv1() (r []T) {
	r, _ = vs.FetchAdv()
	return
}

// FetchBX returns the coordinates of the next chunk in [beg, endx)
// without advancing the iterator.
func (vs *VectorSlicer[T]) FetchBX() (beg, endx int, done bool) {
	if vs.done {
		done = true
		return
	}
	if vs.beg >= vs.endx {
		done = true
		vs.done = true
		return
	}
	endx = vs.beg + vs.chunk
	if endx >= vs.endx {
		endx = vs.endx
	}
	beg = vs.beg
	return
}

// FetchBX returns the coordinates of the next chunk in [beg, endx)
// and then advances the iterator to the next chunk.
func (vs *VectorSlicer[T]) FetchAdvBX() (beg, endx int, done bool) {
	if vs.done {
		done = true
		return
	}
	if vs.beg >= vs.endx {
		done = true
		vs.done = true
		return
	}
	endx = vs.beg + vs.chunk
	if endx >= vs.endx {
		endx = vs.endx
	}
	beg = vs.beg

	// and the advance
	vs.beg = endx

	return
}
