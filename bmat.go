package gocortado

import (
	"fmt"
	"sort"
)

// BoolMatrix is a matrix of bool. Since
// bool is not Addable, we cannot use Matrix[T].
type BoolMatrix struct {
	Nrow int
	Ncol int

	Colnames []string
	Rownames []string

	IsColMajor bool // row major by default
	Dat        []bool

	// For zero-copy extraction of a subset of
	// columns that can be Fetched by chunked
	// rows, we also implement an
	// alterntaive representation that can
	// be layered atop, when ReformatToSliceOfColVec()
	// has been called and it has set IsSliceOfColVec to true.
	//
	// We can refer to columns via a slice
	// of ColVec. If IsSliceOfColVec is true,
	// then IsColMajor will be true too for sure;
	// but its implementations will be overriden.
	//
	IsSliceOfColVec bool
	ColVec          []*ColVec[bool]

	// track metadata by column/row; and don't share
	// with pointers, use values here, so each Matrix
	// gets its own copy, and we can update Colj without
	// impacting the origin Matrix.
	Cmeta []FeatMeta

	// Row meta data is mostly if we Transpose and then
	// Transpose back, we retain the column meta data.
	Rmeta []FeatMeta
}

// CmetaDisplay returns just the essentials of m.Cmeta for diagnostics
func (m *BoolMatrix) CmetaDisplay() (feaDisplay []string) {
	for i := range m.Cmeta {
		feaDisplay = append(feaDisplay, fmt.Sprintf(`%v:FeatMeta{Name:%v, Colj:%v}`, i, m.Cmeta[i].Name, m.Cmeta[i].Colj))
	}
	return
}

// Transpose flips the Matrix without
// changing Dat. It turns m into its transpose efficiently. Only
// meta data describing how to access the rows and columns
// is adjusted, and this is very quick.
// Transpose is not allowed for IsSliceOfColVec:true Matrixes
// and we will panic.
func (m *BoolMatrix) Transpose() {
	if m.IsSliceOfColVec {
		panic("m.IsSliceOfColVec is true, so BoolMatrix.Transpose() is not allowed")
	}
	m.IsColMajor = !m.IsColMajor
	m.Nrow, m.Ncol = m.Ncol, m.Nrow
	m.Rownames, m.Colnames = m.Colnames, m.Rownames
	m.Cmeta, m.Rmeta = m.Rmeta, m.Cmeta
}

// Set v as the value for [i,j]-th element.
func (m *BoolMatrix) Set(i, j int, v bool) {
	if m.IsSliceOfColVec {
		m.ColVec[j].Dat[i] = v
	} else if m.IsColMajor {
		m.Dat[j*m.Nrow+i] = v
	} else {
		m.Dat[i*m.Ncol+j] = v
	}
}

// At reads out the [i,j]-th element.
func (m *BoolMatrix) At(i, j int) bool {
	if i >= m.Nrow {
		panic(fmt.Sprintf("[%v x %v] Matrix.At(i=%v, j=%v): i out of bounds: m.Nrow=%v", m.Nrow, m.Ncol, i, j, m.Nrow))
	}
	if j >= m.Ncol {
		panic(fmt.Sprintf("[%v x %v] Matrix.At(i=%v, j=%v): j out of bounds: m.Ncol=%v", m.Nrow, m.Ncol, i, j, m.Ncol))
	}
	if m.IsSliceOfColVec {
		return m.ColVec[j].Dat[i]
	} else if m.IsColMajor {
		return m.Dat[j*m.Nrow+i]
	} else {
		return m.Dat[i*m.Ncol+j]
	}
}

// Reshape does not change Dat, but re-assigns Nrow = newNrow
// and Ncol = newNcol. It also discards m.Colnames and m.Rownames.
// It will reinitialize Cmeta to be newNcol long; but that looses all
// Cmeta[i].Names and any other meta information that they contained.
// So avoid Reshape unless you can re-create any needed Cmeta information.
// Reshape will panic if m.IsSliceOfColVec is true.
func (m *BoolMatrix) Reshape(newNrow, newNcol int) {
	if m.IsSliceOfColVec {
		panic("Reshape not allowed when Matrix is in IsSliceOfColVec:true format")
	}
	newTot := newNrow * newNcol
	oldTot := m.Nrow * m.Ncol
	if newTot != oldTot {
		panic(fmt.Sprintf("Reshape error: newTot=%v (%v x %v) but oldTot=%v (%v x %v)", newTot, newNrow, newNcol, oldTot, m.Nrow, m.Ncol))
	}
	m.Nrow = newNrow
	m.Ncol = newNcol
	m.initCmeta()
	// give up on Colnames/Rownames if Reshape()ing
	m.Colnames = nil
	m.Rownames = nil
}

// create m.Cmeta, m.Ncol in length. Fill in the Colj entries.
func (m *BoolMatrix) initCmeta() {
	m.Cmeta = make([]FeatMeta, m.Ncol)
	for j := 0; j < m.Ncol; j++ {
		m.Cmeta[j].Colj = j
	}
}

// Row will return the underlying slice from .Dat of row i if the the
// matrix is in row-major order; otherwise it will return
// a coalesced copy and changing res will have no impact on .Dat.
//
// In other words, it will try and do as little work as possible
// to return a readable copy of the data. But if you need to
// write into it... make sure that you have a row-major matrix;
// or use WriteRow to write it back at the end. (And comment out
// the panic that warns about this.
func (m *BoolMatrix) Row(i int) (res []bool) {
	if i < 0 {
		panic("i must not be negative")
	}
	if i >= m.Nrow {
		panic(fmt.Sprintf("i(%v) must be < m.Nrow(%v)", i, m.Nrow))
	}

	if m.IsSliceOfColVec {
		for j := 0; j < m.Ncol; j++ {
			res = append(res, m.ColVec[j].Dat[i])
		}
		return
	}

	if m.IsColMajor {
		if m.Ncol == 1 {
			return m.Dat[i : i+1]
		}
		panic("Row called on multi-column Column-major Matrix. Updates/changes to the resulting slice will not be reflected in the Matrix, since the result is a separate copy, not refering to the original. Be sure this is what you expect! (or just ReformatToRowMajor() this Matrix first, if you needs updates to be visible..")
		for j := 0; j < m.Ncol; j++ {
			res = append(res, m.At(i, j))
		}
		return
	} else {
		return m.Dat[i*m.Ncol : (i+1)*m.Ncol]
	}
}

// RowChunk is like Row, but returns multiple rows in row-major form.
// All columns are returned.
func (m *BoolMatrix) RowChunk(beg, endx int) (r *BoolMatrix) {
	if beg < 0 {
		panic("beg must not be negative")
	}
	if beg >= m.Nrow {
		panic(fmt.Sprintf("beg(%v) must be < m.Nrow(%v)", beg, m.Nrow))
	}
	if endx < 0 {
		panic("endx must not be negative")
	}
	if endx < beg {
		panic(fmt.Sprintf("endx(%v) must be >= beg(%v)", endx, beg))
	}
	if endx > m.Nrow {
		// should we auto clamp, putting endx = m.Nrow here?
		panic(fmt.Sprintf("endx(%v) must be <= m.Nrow(%v)", endx, m.Nrow))
	}

	if m.IsColMajor || m.IsSliceOfColVec {
		if m.Ncol == 1 {

			if m.IsSliceOfColVec {
				r = &BoolMatrix{
					Dat:      m.ColVec[0].Dat[beg:endx],
					Ncol:     1,
					Nrow:     endx - beg,
					Colnames: m.Colnames,
					Cmeta:    m.Cmeta,
				}
			} else {
				r = &BoolMatrix{
					Dat:      m.Dat[beg:endx],
					Ncol:     1,
					Nrow:     endx - beg,
					Colnames: m.Colnames,
					Cmeta:    m.Cmeta,
				}
			}
			if len(m.Rownames) == m.Nrow {
				r.Rownames = m.Rownames[beg:endx]
			}
			return
		} else {
			//panic("RowChunk called on Column-major Matrix. Updates/changes to the Rowset will not be reflected in the Matrix, since the RowChunk returned *Matrix has a copy, not pointers to the original. Be sure this is what you expect! (or just ReformatToRowMajor() this Matrix first, if you needs updates to be visible..")

			r = &BoolMatrix{
				Ncol:     m.Ncol,
				Nrow:     endx - beg,
				Colnames: m.Colnames,
				Cmeta:    m.Cmeta,
			}
			var res []bool
			for i := beg; i < endx; i++ {
				for j := 0; j < m.Ncol; j++ {
					res = append(res, m.At(i, j))
				}
			}
			r.Dat = res
			if len(m.Rownames) == m.Nrow {
				r.Rownames = m.Rownames[beg:endx]
			}
		}
		return
	} else {
		// row-major, leave IsColMajor:false by default.
		r = &BoolMatrix{Dat: m.Dat[beg*m.Ncol : endx*m.Ncol], Ncol: m.Ncol, Nrow: endx - beg, Colnames: m.Colnames}
		if len(m.Rownames) == m.Nrow {
			r.Rownames = m.Rownames[beg:endx]
		}
		return
	}
}

// WriteRow will replace row i with writeme, which must have length m.Ncol.
func (m *BoolMatrix) WriteRow(i int, writeme []bool) {
	if i < 0 {
		panic("i must not be negative")
	}
	if i >= m.Nrow {
		panic(fmt.Sprintf("i(%v) must be < m.Nrow(%v)", i, m.Nrow))
	}
	if len(writeme) != m.Ncol {
		panic(fmt.Sprintf("short row in WriteRow(): writeme len(%v) must be == m.Ncol(%v)", len(writeme), m.Ncol))
	}
	if m.IsColMajor || m.IsSliceOfColVec {
		for j := 0; j < m.Ncol; j++ {
			m.Set(i, j, writeme[j])
		}
	} else {
		// row major
		copy(m.Dat[i*m.Ncol:(i+1)*m.Ncol], writeme)
	}
}

// WriteCol will replace column j with writeme, which must have length m.Row.
func (m *BoolMatrix) WriteCol(j int, writeme []bool) {
	if j < 0 {
		panic("j must not be negative")
	}
	if j >= m.Nrow {
		panic(fmt.Sprintf("j(%v) must be < m.Ncol(%v)", j, m.Ncol))
	}
	if len(writeme) != m.Nrow {
		panic(fmt.Sprintf("short column in WriteCol(): writeme len(%v) must be == m.Nrow(%v)", len(writeme), m.Nrow))
	}

	if m.IsSliceOfColVec {
		copy(m.ColVec[j].Dat, writeme)
	} else if m.IsColMajor {
		// column major
		copy(m.Dat[j*m.Nrow:(j+1)*m.Nrow], writeme)
	} else {
		// row major
		for i := 0; i < m.Nrow; i++ {
			m.Set(i, j, writeme[i])
		}
	}
}

// Col will return the underlying slice from .Dat of column j if the the
// matrix is in column-major order; otherwise it will return
// a coalesced copy and changing res will have no impact on .Dat.
func (m *BoolMatrix) Col(j int) (res []bool) {
	if j < 0 {
		panic("j must not be negative")
	}
	if j >= m.Ncol {
		panic(fmt.Sprintf("j(%v) must be < m.Ncol(%v)", j, m.Ncol))
	}
	if m.IsSliceOfColVec {
		return m.ColVec[j].Dat

	} else if m.IsColMajor {
		return m.Dat[j*m.Nrow : (j+1)*m.Nrow]

	} else {
		for i := 0; i < m.Nrow; i++ {
			res = append(res, m.At(i, j))
		}
		return
	}
}

// NewBoolMatrix allocates room for nrow * ncol elements.
func NewBoolMatrix(nrow, ncol int) (m *BoolMatrix) {
	m = &BoolMatrix{
		Nrow: nrow,
		Ncol: ncol,
		Dat:  make([]bool, nrow*ncol),
	}
	m.initCmeta()
	return
}

// NewBoolMatrixColMajor returns a column-major matrix.
func NewBoolMatrixColMajor(nrow, ncol int) (m *BoolMatrix) {
	m = NewBoolMatrix(nrow, ncol)
	m.IsColMajor = true
	return
}

// NewBoolMatrixColVec allocates room for nrow * ncol elements
// in a IsSliceOfColVec format.
func NewBoolMatrixColVec(nrow, ncol int) (m *BoolMatrix) {
	m = &BoolMatrix{
		Nrow:            nrow,
		Ncol:            ncol,
		Dat:             make([]bool, nrow*ncol),
		IsColMajor:      true,
		IsSliceOfColVec: true,
		ColVec:          make([]*ColVec[bool], ncol),
		Cmeta:           make([]FeatMeta, ncol),
	}
	for j := range m.ColVec {
		cv := &ColVec[bool]{}
		cv.Dat = m.Dat[j*nrow : (j+1)*nrow]
		m.ColVec[j] = cv
	}
	m.initCmeta()
	return
}

// String satisfies the common Stringer interface.
// It provides a view of the contents of the Matrix m.
func (m *BoolMatrix) String() (r string) {

	r = fmt.Sprintf("BoolMatrix(Nrow=%v x Ncol=%v) = [\n", m.Nrow, m.Ncol)

	// since with various re-shaping, the Rownames and/or Colnames might
	// not have kept up, only try to print them if they are still in sync
	// with the current shape.
	haveRownames := len(m.Rownames) == m.Nrow
	if len(m.Colnames) == m.Ncol {
		// show the header
		if haveRownames {
			r += `"rowid", `
		}
		for j := 0; j < m.Ncol; j++ {
			if j == m.Ncol-1 {
				r += `"` + m.Colnames[j] + "\"\n"
			} else {
				r += `"` + m.Colnames[j] + "\", "
			}
		}
	}
	// only show max of 20 rows
	for i := 0; i < m.Nrow && i < 20; i++ {
		line := " "
		if haveRownames {
			line = `"` + m.Rownames[i] + `", `
		}
		for j := 0; j < m.Ncol; j++ {
			line += fmt.Sprintf("%v", m.At(i, j))
			if j < m.Ncol-1 {
				line += ", "
			} else {
				line += "\n"
			}
		}
		r += line
	}
	if m.Nrow > 20 {
		r += fmt.Sprintf(" ... (rows 20:%v not shown) ...\n", m.Nrow)
	}
	r += "]\n"
	return
}

// FillRowMajor copies slc into Dat, and sets IsColMajor to false.
// If makeCopy then we'll make our own copy of slc; otherwise just point to it.
func (m *BoolMatrix) FillRowMajor(slc []bool, makeCopy bool) {
	if makeCopy {
		copy(m.Dat, slc)
	} else {
		m.Dat = slc
	}
	m.IsColMajor = false
	m.IsSliceOfColVec = false
	m.ColVec = m.ColVec[:0]
}

// FillColMajor copies slc into Dat, and sets IsColMajor to true
// If makeCopy then we'll make our own copy of slc; otherwise just point to it.
func (m *BoolMatrix) FillColMajor(slc []bool, makeCopy bool) {
	if makeCopy {
		copy(m.Dat, slc)
	} else {
		m.Dat = slc
	}
	m.IsColMajor = true
}

// Clone returns a fresh copy of m, with no shared state.
func (m *BoolMatrix) Clone() (clone *BoolMatrix) {
	clone = &BoolMatrix{
		Nrow: m.Nrow,
		Ncol: m.Ncol,

		Colnames: make([]string, len(m.Colnames)),
		Rownames: make([]string, len(m.Rownames)),

		IsColMajor:      m.IsColMajor,
		IsSliceOfColVec: m.IsSliceOfColVec,

		ColVec: make([]*ColVec[bool], len(m.ColVec)),

		Cmeta: make([]FeatMeta, m.Ncol),
	}
	if m.IsSliceOfColVec {
		// we might just have a subset of columns
		// some original Dat...if so, condense as we
		// copy into clone. This
		// does less copying, and also allows the
		// ColVec setup logic below to work without
		// alot of pointer arithmetic to figure out
		// which columns should be skipped.
		nrow := m.Nrow
		clone.Dat = make([]bool, nrow*clone.Ncol)
		for j, mcv := range m.ColVec {
			//vv("copying from mcv.Dat len %v", len(mcv.Dat))
			copy(clone.Dat[j*nrow:(j+1)*nrow], mcv.Dat)
		}
		for j := range clone.ColVec {
			cv := &ColVec[bool]{}
			cv.Dat = clone.Dat[j*nrow : (j+1)*nrow]
			clone.ColVec[j] = cv
		}
	} else {
		clone.Dat = make([]bool, len(m.Dat))
		copy(clone.Dat, m.Dat)
	}
	copy(clone.Colnames, m.Colnames)
	copy(clone.Rownames, m.Rownames)
	copy(clone.Cmeta, m.Cmeta)

	return
}

// AddRow extends the matrix by one row and returns the index to the new row.
// The new row is all 0. This can be pretty fast if m is row major; and can
// be pretty slow if not. Pass empty string for rowlabel if not using them.
func (m *BoolMatrix) AddRow(rowlabel string) (i int) {

	if m.IsSliceOfColVec {
		for j := range m.ColVec {
			m.ColVec[j].Dat = append(m.ColVec[j].Dat, false)
		}
		i = m.Nrow
		m.Nrow++

	} else if m.IsColMajor {
		clone := m.Clone()
		i = m.Nrow
		m.Nrow++
		m.Dat = make([]bool, m.Nrow*m.Ncol)
		for j := 0; j < m.Ncol; j++ {
			for i := 0; i < m.Nrow-1; i++ {
				m.Dat[j*m.Nrow+i] = clone.At(i, j)
			}
		}
	} else {
		m.Dat = append(m.Dat, make([]bool, m.Ncol)...)
		i = m.Nrow
		m.Nrow++
	}
	if rowlabel == "" {
		// not using Rownames, skip append
	} else {
		m.Rownames = append(m.Rownames, rowlabel)
	}
	return
}

// ReformatToSliceOfColVec will set IsSliceOfColVec to
// true after reformating the data internally to use
// the SliceOfColVec representation. To do so we'll
// call ReformatToColumnMajor() which will involve a
// copy if the Matrix starts out row-major.
func (m *BoolMatrix) ReformatToSliceOfColVec() {
	if m.IsSliceOfColVec {
		// already there
		return
	}
	// make sure we are column major
	m.ReformatToColumnMajor()
	m.IsSliceOfColVec = true
	// There is no copying once we are column major;
	// we just point to the column boundaries for
	// column j with m.ColVec[j]. The backing
	// array is that same as that for m.Dat.
	m.ColVec = make([]*ColVec[bool], m.Ncol)
	for j := range m.ColVec {
		cv := &ColVec[bool]{
			Dat: m.Dat[j*m.Nrow : (j+1)*m.Nrow],
		}
		m.ColVec[j] = cv
	}
}

// ReformatToColumnMajor will actually re-write the data in .Dat, if need be,
// to be column major: to have each columns's data adjacent so
// advancing the index of .Dat by 1 goes to the next row; or
// to the top of the next column if at the last row.
//
// Be aware that the Row() fetches from m will be slower; but reading
// a whole column will be faster of course.
//
// This is a no-op if the Matrix already has IsColMajor true.
func (m *BoolMatrix) ReformatToColumnMajor() {
	if m.IsColMajor {
		return
	}
	clone := m.Clone()

	for j := 0; j < m.Ncol; j++ {
		for i := 0; i < m.Nrow; i++ {
			m.Dat[j*m.Nrow+i] = clone.At(i, j)
		}
	}
	m.IsColMajor = true
}

// ReformatToRowMajor will actually re-write the data in .Dat, if need be,
// to be row major: to have each rows's data adjacent so
// advancing the index of .Dat by 1 goes to the next column; or
// to the beginning of the next row if at the last column.
// This is a no-op if the Matrix already has IsColMajor false.
func (m *BoolMatrix) ReformatToRowMajor() {
	if !m.IsColMajor && !m.IsSliceOfColVec {
		// already row major
		return
	}
	//vv("m.Ncol=%v, m.Nrow=%v, m.Dat len = %v", m.Ncol, m.Nrow, len(m.Dat))
	clone := m.Clone()
	//vv("clone.Ncol=%v, clone.Nrow=%v, clone.Dat len = %v", clone.Ncol, clone.Nrow, len(clone.Dat))

	// might have been too small/scattered after a Cbind.
	// Be sure we have enough space ourselves.
	m.Dat = make([]bool, m.Nrow*m.Ncol)
	for i := 0; i < m.Nrow; i++ {
		for j := 0; j < m.Ncol; j++ {
			m.Dat[i*m.Ncol+j] = clone.At(i, j)
		}
	}
	m.IsColMajor = false
	m.IsSliceOfColVec = false
	m.ColVec = m.ColVec[:0]
}

// BoolRowIter refers to a row-slice of a BoolMatrix; see
// the GetRowIter() method on the Matrix below.
type BoolRowIter struct {
	m       *BoolMatrix
	beg     int // the next Fetch() or FetchAdv() will return starting with this row.
	endxrow int // allows us to stop early and not read all of vec, if desired.
	chunk   int
}

// NewRowIter makes a row iterator. See also the method GetRowIter on Matrix.
func NewBoolRowIter(m *BoolMatrix, beg, length, chunk int) *BoolRowIter {
	endx := beg + length
	if endx > m.Nrow {
		endx = m.Nrow
	}
	return &BoolRowIter{
		m:       m,
		beg:     beg,
		endxrow: endx,
		chunk:   chunk,
	}
}

// FetchBX returns the current chunk of rows, pointed to by the [beg, endx)
// returned values. The returned range is empty iff done is returned true;
// so always check done first.
//
// Concretely, the length of the returned range is always endx - beg;
// so [0, 0) is an empty range. The size of the range will be ri.chunk unless
// there are insufficient elements left before hitting the endxrow point.
//
// See also FetchAdvBX to read the current chunk and then advance to the next.
//
// If done is returned true, then beg and endx are undefined and should
// be ignored.
func (ri *BoolRowIter) FetchBX() (beg, endx int, done bool) {
	if ri.beg >= ri.endxrow {
		done = true
		return
	}
	endx = ri.beg + ri.chunk
	if endx >= ri.endxrow {
		endx = ri.endxrow
	}
	beg = ri.beg
	return
}

// FetchAdvBX does FetchBX() and then advances the iterator to the next chunk.
//
// Specifically, FetchAdvBX returns the current chunk of rows,
// pointed to by the [beg, endx) return values, and then
// advances the iterator to the next chunk of rows to be read.
//
// The length of the returned range is always endx - beg;
// so [0, 0) is an empty range. The size of the range will be ri.chunk unless
// there are insufficient elements left before hitting the endxrow point.
//
// The returned range is empty iff done is returned true; so always check done first.
// See also FetchAdv to get a row range without advancing the iterator.
//
// If done is returned true, then beg and endx are undefined and should
// be ignored.
func (ri *BoolRowIter) FetchAdvBX() (beg, endx int, done bool) {
	if ri.beg >= ri.endxrow {
		done = true
		return
	}
	endx = ri.beg + ri.chunk
	if endx >= ri.endxrow {
		endx = ri.endxrow
	}
	beg = ri.beg

	// and the advance
	ri.beg = endx

	return
}

// Fetch returns the current row set, without advancing
func (ri *BoolRowIter) Fetch() (r *BoolMatrix, done bool) {
	if ri.beg >= ri.endxrow {
		done = true
		return
	}
	endx := ri.beg + ri.chunk
	if endx >= ri.endxrow {
		endx = ri.endxrow
	}
	r = ri.m.RowChunk(ri.beg, endx)
	return
}

// Adv advances the row iterator
func (ri *BoolRowIter) Adv() (done bool) {
	if ri.beg >= ri.endxrow-1 {
		done = true
		ri.beg = ri.endxrow
		return
	}
	ri.beg += ri.chunk
	if ri.beg >= ri.endxrow {
		ri.beg = ri.endxrow
	}
	return
}

// return current row and then advance, so the next Fetch or FetchAdv
// will read starting with the beg row.
func (ri *BoolRowIter) FetchAdv() (r *BoolMatrix, done bool) {
	if ri.beg >= ri.endxrow {
		done = true
		return
	}
	endx := ri.beg + ri.chunk
	if endx >= ri.endxrow {
		endx = ri.endxrow
	}
	r = ri.m.RowChunk(ri.beg, endx)
	ri.beg = endx
	return
}

// FetchAdv1 just returns nil if done, without a separate done
// flag. Otherwise identical to FetchAdv() which it calls.
func (ri *BoolRowIter) FetchAdv1() (r *BoolMatrix) {
	r, _ = ri.FetchAdv()
	return
}

// FetchBegEndx just supplies the beg and endx row index that Fetch
// would return. This can be used to coordinate/compare with other
// row iterators or the VectorSlicer.
func (ri *BoolRowIter) FetchBegEndx() (beg, endx int, done bool) {
	if ri.beg >= ri.endxrow {
		done = true
		beg = ri.endxrow
		endx = ri.endxrow
		return
	}
	e := ri.beg + ri.chunk
	if e >= ri.endxrow {
		e = ri.endxrow
	}
	beg = ri.beg
	endx = e
	return
}

// GetRowIter returns an iterator that will read [beg, endxrow) rows of m, by
// requesting Rowset()s of chunk rows at a time. The endxrow parameter
// allows us to read fewer than m.Nrow elements all in, if desired.
//
// Single column vectors are supported so that Matrix can be used the chunk
// out simple vectors too. The only current restriction
// is that we will return *all* the columns in our rowset, so to omit
// columns you may need to DeleteCols to adjust the shape of m before hand;
// say to remove any target column, for example. Or just use a
// RowColIter instead.
func (m *BoolMatrix) GetRowIter(begrow, endxrow, chunk int) (r *BoolRowIter) {
	if chunk <= 1 {
		chunk = 1 // minimum fetch size is 1
	}

	// Insist on sanity so the returned Row slices are row major if more than one column.
	// If only a single column, then it does not matter since its all the same.
	if chunk > 1 && m.IsColMajor && m.Ncol > 1 {
		panic(fmt.Sprintf("ugh. chunk=%v and GetRowIter called on a multi-column(%v) Column-major Matrix; with more than 1 column, we only support Row-major matrices to meet client expectations of a row being contiguous", chunk, m.Ncol))
	}
	if endxrow > m.Nrow {
		endxrow = m.Nrow
	}
	r = &BoolRowIter{
		m:       m,
		beg:     begrow,
		endxrow: endxrow,
		chunk:   chunk,
	}
	return
}

// DeleteCols deletes from m the 0-based column numbers listed in wcol.
func (m *BoolMatrix) DeleteCols(wcol []int) {
	if len(wcol) == 0 {
		return
	}
	// origin number of columns
	nc0 := m.Ncol

	keepMap := make(map[int]bool)
	for i := 0; i < m.Ncol; i++ {
		keepMap[i] = true
	}
	for w, j := range wcol {
		if j < 0 || j >= m.Ncol {
			panic(fmt.Sprintf("wcol[%v]=%v was out of bounds; <0 or >= m.Ncol(%v)", w, j, m.Ncol))
		}
		delete(keepMap, j)
	}
	var keep []int
	for i := 0; i < m.Ncol; i++ {
		if keepMap[i] {
			keep = append(keep, i)
		}
	}
	sort.Ints(keep)

	if m.IsSliceOfColVec {
		// we can avoid all copies, so do so;
		// we leave Dat alone, even though it might now
		// be bigger than we need be. Clone accounts for this,
		// and will do the condensation if need be. Hence
		// use Clone() afte this if you also want to force a
		// (costly in terms of copying) condensation.

		left := len(keep)
		var cvs []*ColVec[bool]
		for _, j := range keep {
			cvs = append(cvs, m.ColVec[j])
		}
		m.ColVec = cvs

		// and the Colnames / FeatMeta
		if len(m.Colnames) == m.Ncol {
			var cn1 []string
			for _, j := range keep {
				cn1 = append(cn1, m.Colnames[j])
			}
			m.Colnames = cn1
		}
		m.Ncol = left

		// continue down to copy Cmeta too
	} else {

		clone := m.Clone()

		left := len(keep)
		m.Ncol = left
		m.Dat = m.Dat[:m.Ncol*m.Nrow]

		if m.IsColMajor {
			for newj, oldj := range keep {
				for i := 0; i < m.Nrow; i++ {
					m.Dat[newj*m.Nrow+i] = clone.At(i, oldj)
				}
			}
		} else {
			// row major
			for i := 0; i < m.Nrow; i++ {
				for newj, oldj := range keep {
					m.Dat[i*m.Ncol+newj] = clone.At(i, oldj)
				}
			}
		}

		// and the Colnames / FeatMeta
		if len(clone.Colnames) == clone.Ncol {
			m.Colnames = []string{}
			for _, oldj := range keep {
				m.Colnames = append(m.Colnames, clone.Colnames[oldj])
			}
		}
	}

	if len(m.Cmeta) == 0 {
		//vv("m.Cmeta is empty; DeleteCols() ignoring it")

	} else {
		if len(m.Cmeta) != nc0 {
			panic(fmt.Sprintf("DeleteCols problem: pre-reduction len(m.Cmeta)=%v but nc0 = %v", len(m.Cmeta), nc0))
		}
		var newFeatMeta []FeatMeta
		for _, oldj := range keep {
			newFeatMeta = append(newFeatMeta, m.Cmeta[oldj])
		}
		m.Cmeta = newFeatMeta
	}
}

// Cbind will append the columns of m2 on to the right side of m, updating m in-place.
//
// The resulting Matrix m will have m.IsColMajor:true AND m.IsSliceOfColVec:true.
//
// In some cases, e.g. if both m and m2 started as IsColMajor:true, then no .Dat
// will be copied and m will simply point to m2's data. Beware of
// this aliasing. If you change m2 after a Cbind, then those changes to m2 may
// show up also in m. For safety, do not write to m2 after Cbind()-ing it to m.
// Instead, if need be, write through m to the appended columns.
func (m *BoolMatrix) Cbind(m2 *BoolMatrix) {
	if m.Nrow != m2.Nrow {
		panic(fmt.Sprintf("On Cbind(), Nrow must agree! m.Nrow(%v) != m2.Nrow(%v)", m.Nrow, m2.Nrow))
	}

	// origin number of columns
	nc0 := m.Ncol

	newCmeta := make([]FeatMeta, m.Ncol+m2.Ncol)
	copy(newCmeta[:nc0], m.Cmeta)
	copy(newCmeta[nc0:], m2.Cmeta)
	m.Cmeta = newCmeta
	// repair the m2.Cmeta which will have the wrong Colj now, and the wrong MyMat
	for j := range m.Cmeta {
		m.Cmeta[j].Colj = j
		m.Cmeta[j].MyMat = m
	}

	if !m.IsColMajor {
		//vv("m was row major")
		m.ReformatToColumnMajor()
	}
	if !m2.IsColMajor {
		//vv("m2 was row major")
		m2.ReformatToColumnMajor()
	}

	//vv("m pre cbind = '%v'", m)
	//vv("m2 pre cbind = '%v'", m2)

	// INVAR: m.IsColMajor && m2.IsColMajor

	if m.IsSliceOfColVec {

		if m2.IsSliceOfColVec {
			//  m.IsSliceOfColVec:true
			// m2.IsSliceOfColVec:true

			for j := nc0; j < m.Ncol; j++ {
				m.ColVec = append(m.ColVec, m2.ColVec...)
			}
		} else {
			//  m.IsSliceOfColVec:true
			// m2.IsSliceOfColVec:false

			for j := 0; j < m2.Ncol; j++ {
				m.ColVec = append(m.ColVec, &ColVec[bool]{Dat: m2.Dat[j*m2.Nrow : (j+1)*m2.Nrow]})
			}
		}
	} else {

		// we still avoid all copying of .Dat ... just point to the data.
		if !m2.IsSliceOfColVec {
			//  m.IsSliceOfColVec:false
			// m2.IsSliceOfColVec:false

			// just convert to SliceOfColVec format, to avoid further .Dat copies.
			m.IsSliceOfColVec = true
			m.Ncol = nc0 + m2.Ncol
			m.ColVec = make([]*ColVec[bool], m.Ncol)
			//vv("m.Ncol = %v", m.Ncol)
			k := 0
			//vv("len m.Dat  = '%v'", len(m.Dat))
			//vv("len m2.Dat = '%v'", len(m2.Dat))
			for j := range m.ColVec {
				cv := &ColVec[bool]{}
				m.ColVec[j] = cv
				//vv("j = %v, cv=%#v", j, cv)
				if j < nc0 {
					cv.Dat = m.Dat[j*m.Nrow : (j+1)*m.Nrow]
				} else {
					cv.Dat = m2.Dat[k*m.Nrow : (k+1)*m.Nrow]
					k++
				}
			}

		} else {
			//  m.IsSliceOfColVec:false
			// m2.IsSliceOfColVec:true

			// convert m to IsSliceOfColVec
			m.IsSliceOfColVec = true
			m.ColVec = make([]*ColVec[bool], nc0)
			for j := range m.ColVec {
				cv := &ColVec[bool]{}
				m.ColVec[j] = cv
				cv.Dat = m.Dat[j*m.Nrow : (j+1)*m.Nrow]
			}
			m.ColVec = append(m.ColVec, m2.ColVec...)
			m.Ncol = nc0 + m2.Ncol
		}
	}
	m.Ncol = nc0 + m2.Ncol
	if len(m.Colnames) == nc0 && len(m2.Colnames) == m2.Ncol {
		m.Colnames = append(m.Colnames, m2.Colnames...)
		//vv("cbind: new colnames = '%v'", m.Colnames)
	} else {
		//vv("cbind: NO new colnames = '%v'", m.Colnames)
	}
}

// BoolRowColIter is a BoolMatrix iterator that returns, upon Fetch, chunks of rows in
// a sub-matrix of the specified colums (factors).
type BoolRowColIter struct {
	// origin of data
	m *BoolMatrix

	// rows to pick
	beg     int // the next Fetch() or FetchAdv() will return starting with this row.
	endxrow int // allows us to stop early and not read all of vec, if desired.
	chunk   int
	name    string

	// cols to pick
	col []int

	fea []FeatMeta
}

// NewRowColIter specifies the columns to fetch via the factors slice.
func (m *BoolMatrix) NewRowColIter(factors []FeatMeta, begrow, length, chunk int, name string) (rci *BoolRowColIter) {
	//vv("len factors = %v", len(factors))
	//vv("m.Ncol = %v;  len(%v)colnames = '%v'", m.Ncol, len(m.Colnames), m.Colnames)
	var col []int
	for k, f := range factors {
		col = append(col, f.Colj)
		if f.Name != m.Colnames[f.Colj] {
			panic(fmt.Sprintf("f.Name='%v' but m.Colnames[f.Colj=%v]='%v'", f.Name, f.Colj, m.Colnames[f.Colj]))
		}
		_ = k
		//vv("NewRowColIter on creation, k=%v, f.Name = '%v', f.Colj = %v", k, f.Name, f.Colj)
	}

	endxrow := begrow + length

	if chunk <= 1 {
		chunk = 1 // minimum fetch size is 1
	}

	// Insist on sanity so the returned Row slices are row major if more than one column.
	// If only a single column, then it does not matter since its all the same.
	//if chunk > 1 && m.IsColMajor && m.Ncol > 1 {
	//	panic(fmt.Sprintf("ugh. chunk=%v and GetRowIter called on a multi-column(%v) Column-major Matrix; with more than 1 column, we only support Row-major matrices to meet client expectations of a row being contiguous", chunk, m.Ncol))
	//}
	if endxrow > m.Nrow {
		endxrow = m.Nrow
	}
	rci = &BoolRowColIter{
		m:       m,
		beg:     begrow,
		endxrow: endxrow,
		chunk:   chunk,
		col:     col,
		fea:     factors,
		name:    name,
	}
	return
}

// FetchAdv1 is the basic iterator operation but without the done
// return value. The returned r will be nil when there are no
// more rows to return. See also FetchAdv() which FetchAdv1()
// calls internally.
func (rci *BoolRowColIter) FetchAdv1() (r *BoolMatrix) {
	r, _ = rci.FetchAdv()
	return
}

// FetchAdv is the basic iterator operation. Returns a submatrix r
// that has a subset of columns and a chunk of contigious rows from m.
func (rci *BoolRowColIter) FetchAdv() (r *BoolMatrix, done bool) {
	//vv("RowColIter FetchAdv called. rci.beg=%v; rci.endxrow=%v; rci.chunk=%v", rci.beg, rci.endxrow, rci.chunk)
	if rci.beg >= rci.endxrow {
		done = true
		return
	}
	endx := rci.beg + rci.chunk
	if endx >= rci.endxrow {
		endx = rci.endxrow
	}

	r = rci.m.ExtractRowsColsAsMatrix(rci.beg, endx, rci.col)

	//vv("len r.Cmeta = '%v'", len(r.Cmeta))
	//vv("len(rci.fea) = '%#v'", len(rci.fea))

	r.Cmeta = append([]FeatMeta{}, rci.fea...)

	//vv("rci.FetchAdv ('%v') returning matrix rows [%v, %v)  and rci.col = '%v' -> matrix r returned is: '%v'", rci.name, rci.beg, endx, rci.col, r)

	// sanity check features are named in expected order
	//for j, f := range rci.fea {
	//	vv("j=%v, fea = '%v'", j, f.Name)
	//}
	//vv("rci.FetchAdv ('%v') returning matrix with r.Colnames = '%v'", rci.name, r.Colnames)
	for j, f := range rci.fea {
		rcj := r.Colnames[j]
		if rcj != f.Name {
			panic(fmt.Sprintf("at j=%v, have f.Name='%v' from ri.fea, but r.Colnames[j=%v]='%v'", j, f.Name, j, rcj))
		}
	}

	//vv("rci.FetchAdv advancing rci.beg from %v -> %v == endx", rci.beg, endx)
	rci.beg = endx
	return
}

// ExtractRowsColsAsMatrix creates a submatrix of the requested rows
// and columns. This is zero copy if m.IsSliceOfColVec is true.
func (m *BoolMatrix) ExtractRowsColsAsMatrix(rowbeg, rowendx int, wcol []int) (r *BoolMatrix) {

	//vv("ExtractRowsColsAsMatrix( wcol= '%#v'); m.Nrow=%v, m.Ncol=%v; rowbeg=%v, rowendx=%v ; m.Colnames='%v'", wcol, m.Nrow, m.Ncol, rowbeg, rowendx, m.Colnames)

	uniq := make(map[int]bool)
	for wj, j := range wcol {
		_, already := uniq[j]
		if already {
			panic(fmt.Sprintf("ExtractRowsColsAsMatrix error: duplicate wcol entry found for j=%v at pos %v", j, wj))
		}
		uniq[j] = true

		if j < 0 {
			panic(fmt.Sprintf("j(%v) at pos %v cannot be negative", j, wj))
		}
		if j >= m.Ncol {
			panic(fmt.Sprintf("j(%v) at pos %v must be < m.Ncol(%v)", j, wj, m.Ncol))
		}
	}
	if rowendx > m.Nrow {
		panic(fmt.Sprintf("ExtractRowsColsAsMatrix() call error: rowendx(%v) > m.Nrow(%v)", rowendx, m.Nrow))
	}
	if rowbeg >= m.Nrow {
		panic(fmt.Sprintf("ExtractRowsColsAsMatrix() call error: rowbeg(%v) >= m.Nrow(%v)", rowbeg, m.Nrow))
	}
	if rowbeg < 0 {
		panic("rowbeg cannot be negative")
	}
	if rowendx <= 0 {
		panic("rowendx must be positive")
	}
	// cannot sort! client expects back in the order they requested them.
	//sort.Ints(wcol)

	nrow := rowendx - rowbeg
	ncol := len(wcol)

	// only bother with colnames if m.Colnames is not messed up.
	var colnames []string
	cmeta := make([]FeatMeta, ncol)

	for newj, oldj := range wcol {
		cmeta[newj] = m.Cmeta[oldj]
		if len(m.Colnames) == m.Ncol {
			cnj := m.Colnames[oldj]
			//vv("on newj=%v; oldj = %v, we see colname cnj = '%v' from m", newj, oldj, cnj)
			colnames = append(colnames, cnj)
			if cmeta[newj].Name != cnj {
				panic(fmt.Sprintf("name / column disagreement: cmeta[newj=%v].Name='%v' but cnj='%v'; oldj=%v", newj, cmeta[newj].Name, cnj, oldj))
			}
		}
	}

	if m.IsSliceOfColVec {
		cvs := make([]*ColVec[bool], ncol)
		for newj, oldj := range wcol {
			cv := &ColVec[bool]{}
			cv.Dat = m.ColVec[oldj].Dat[rowbeg:rowendx]
			cvs[newj] = cv
		}
		r = &BoolMatrix{
			IsColMajor:      true,
			IsSliceOfColVec: true,
			Colnames:        colnames,
			Nrow:            nrow,
			Ncol:            ncol,
			Dat:             m.Dat,
			ColVec:          cvs,
			Cmeta:           cmeta,
		}
		return
	}

	xx := make([]bool, ncol*nrow)

	for i := 0; i < nrow; i++ {
		for wj, j := range wcol {
			xx[i*ncol+wj] = m.At(rowbeg+i, j)
		}
	}

	r = &BoolMatrix{
		Colnames: colnames,
		Nrow:     nrow,
		Ncol:     ncol,
		Dat:      xx,
		Cmeta:    cmeta,
	}

	return
}

// ExtractFeatAsMatrix returns a sub-Matrix of m that
// has all rows but only the columns associated with factors.
func (m *BoolMatrix) ExtractFeatAsMatrix(factors []FeatMeta) (r *BoolMatrix) {

	var col []int
	uniq := make(map[int]bool)

	for k, f := range factors {

		_, already := uniq[f.Colj]
		if already {
			panic(fmt.Sprintf("ExtractFeatAsMatrix error: duplicate col entry found for k=%v -> f.Colj=%v for factor '%v'", k, f.Colj, f.Name))
		}
		col = append(col, f.Colj)
		uniq[f.Colj] = true

		if f.Name != m.Colnames[f.Colj] {
			panic(fmt.Sprintf("f.Name='%v' but m.Colnames[f.Colj=%v]='%v'", f.Name, f.Colj, m.Colnames[f.Colj]))
		}
		_ = k

		j := f.Colj

		if j < 0 {
			panic(fmt.Sprintf("j(%v) at feat '%v' cannot be negative", j, f.Name))
		}
		if j >= m.Ncol {
			panic(fmt.Sprintf("j(%v) at feat '%v' must be < m.Ncol(%v)", j, f.Name, m.Ncol))
		}
	}

	// default to all rows
	rowbeg := 0
	rowendx := m.Nrow

	nrow := rowendx - rowbeg
	ncol := len(col)

	var colnames []string
	for _, j := range col {
		cnj := m.Colnames[j]
		//vv("on j = %v, we see colname cnj = '%v' from m", j, cnj)
		colnames = append(colnames, cnj)
	}
	cmeta := make([]FeatMeta, len(factors))
	copy(cmeta, factors)

	if m.IsSliceOfColVec {
		cvs := make([]*ColVec[bool], ncol)
		for newj, oldj := range col {
			cv := &ColVec[bool]{}
			cv.Dat = m.ColVec[oldj].Dat[rowbeg:rowendx]
			cvs[newj] = cv
		}
		r = &BoolMatrix{
			IsColMajor:      true,
			IsSliceOfColVec: true,
			Colnames:        colnames,
			Nrow:            nrow,
			Ncol:            ncol,
			Dat:             m.Dat,
			ColVec:          cvs,
			Cmeta:           cmeta,
		}
		return
	}

	xx := make([]bool, ncol*nrow)

	for i := 0; i < nrow; i++ {
		for wj, j := range col {
			xx[i*ncol+wj] = m.At(rowbeg+i, j)
		}
	}

	r = &BoolMatrix{
		Colnames: colnames,
		Nrow:     nrow,
		Ncol:     ncol,
		Dat:      xx,
		Cmeta:    cmeta,
	}

	return

}
