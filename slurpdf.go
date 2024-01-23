package gocortado

import (
	"bytes"
	//"compress/gzip"
	//"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type ColumnKind int

const (
	FACTOR  ColumnKind = 1
	NUMERIC ColumnKind = 2
)

// match the reference implement for a missing factor level; always level == 0
const MISSINGLEVEL string = "."

// SlurpDataFrame handles two type of data frames: those with all float64,
// and those with string columns. String columns are encoded into uint16
// factor matrix.
//
// The all float64 reading takes in a comma-separated-value (csv) files
// that has a special  structure. After the header, the first two columns
// are expected to two contains strings, (a timestamp and symbol string, typically);
// and then all of the rest of the columns must
// be float64 values.
//
// Since most of the work is parsing the float64,
// we try to do that in parallel and using large
// blocks of contiguous memory to allow the CPU
// caches and pipelining to be effective. We
// memory map the file to effect this.
//
// When a .gz file path is supplied, this cannot be memory mapped; so
// we read it using the csv libraries, which can be slower.
type SlurpDataFrame struct {

	// nheader = number of fields in the header; nCol will have 2 less for the matrix,
	// since the matrix lacks the first 2 fields which are strings.
	Nheader int

	// the full header, as a single string. Fields separated by commas.
	Header string

	// the header broken out into fields.
	// includes tm,sym as the first two, so is 2 more than nCol, typically;
	// assuming they were present in the original header.
	Colnames []string

	// matching exactly the columns of Matrix, Ncol long
	MatrixColnames []string

	// the numeric, float64 data.
	Matrix []float64

	// number of numeric data colums in matrix (not counting tm,sym)
	Ncol int

	// number of rows (not counting the header)
	Nrow int

	// Just the symbol (2nd column), from the first row.
	// They are probably all the same anyway.
	Sym string

	// protect parseTot which counts how many float64 we
	// have parsed in parallel goroutines; during Slurp()
	mut      sync.Mutex
	parseTot int

	// the timestamps on the rows
	Tm []time.Time

	Frompath string

	// if the 2 string columns are missing
	Missing2strings bool

	// end of all float64 representation (the original).

	// begin the new "factor" handling representation, using
	// our generic Matrix[type] from mat.go.

	// Instead of being all numeric features, instead we have
	// two parts, numeric features in NumericMat, and factors
	// in FactorMat, and if they were originally interlaced, they
	// are separated out into their own kind of matrix now.
	HasFactors  bool
	Kindvec     []ColumnKind
	tradNumeric map[int]bool
	tradFactors map[int]bool

	numericNames []string
	numericCols  []int
	factorNames  []string
	factorCols   []int

	// how the string factors are encoded numerically
	factorMaps []map[string]int

	// how to go from numeric code back to original string in .csv file.
	invFactorMaps []map[int]string

	NumericMat *Matrix[float64]

	// Because we do not know how many factors we will need, and
	// because the initial converts all real features to factors,
	// we will initially deploy the FactorMat will a full int (64-bit integer)
	// work of factor room. Later, perhaps, this can be reduced to uint16 or uint8,
	// but that requires domain knowledge of the features at hand
	// on a case-by-case basis. For now we give ourselves a fighting
	// chance of handling any real-value feature with the full
	// generality of int numbered factors.
	FactorMat *Matrix[int]
}

var ErrNoHeader = fmt.Errorf("no header")

var ErrNoData = fmt.Errorf("no data")
var ErrRowNotFound = fmt.Errorf("row not found")

func NewSlurpDataFrame(missing2strings bool) *SlurpDataFrame {
	return &SlurpDataFrame{
		Missing2strings: missing2strings,
	}
}

// get element from the matrix, ignoring the first 2 string columns if they exist.
// irow and jcol are 0 based.
func (df *SlurpDataFrame) MatrixAt(irow, jcol int) float64 {
	return df.Matrix[irow*df.Ncol+jcol]
}

func (df *SlurpDataFrame) MatFullRow(irow int) []float64 {
	return df.Matrix[irow*df.Ncol : ((irow + 1) * df.Ncol)]
}

// returns the first leftCount elements of irow; useful to pick
// out just the training data if it is all on the left side of
// the matrix.
func (df *SlurpDataFrame) MatPartRow(irow, leftCount int) []float64 {
	b := irow * df.Ncol
	return df.Matrix[b : b+leftCount]
}

// Disgorge writes the matrix/data-frame back to disk.
// As you might guess, this is really slow. It is useful,
// however, to show that we parsed the original correctly,
// and can reconstruct it precisely if need be.
func (df *SlurpDataFrame) Disgorge(path string) (err error) {

	// reconstruct the matrix for comparison

	w, err := os.Create(path)
	panicOn(err)
	defer w.Close()
	w.WriteString(df.Header)
	w.WriteString("\n")
	k := 0

	for i := 0; i < df.Nrow; i++ {
		var line string
		if df.Missing2strings {
			// no tm,sym,
		} else {
			tm := df.Tm[i]
			line = tm.Format(time.RFC3339Nano) + ","
			line += string(df.Sym) + ","
		}
		for j := 0; j < df.Ncol; j++ {
			ele := df.Matrix[k]
			//sele := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%0.6f", ele), "0"), ".")
			sele := fmt.Sprintf("%v", ele)
			if j == 0 {
				line += sele
			} else {
				line += "," + sele
			}
			k++
		}
		w.WriteString(line + "\n")
	}

	return
}

// ReadGzipped is used when we have a compressed csv file we
// cannot directly memory map.
func (df *SlurpDataFrame) ReadGzipped(path string) (err error) {
	t0 := time.Now()
	lines := 1 // saw the header at 0 already

	defer func() {
		vv("ReadGzipped path='%v' took '%v'; we see %v lines", path, time.Since(t0), lines)
	}()
	loader, err := NewCsvLoader2(path)
	if err != nil {
		return err
	}
	df.Frompath = path

	var r [][]string
	var nfields int
	for {
		rec, err := loader.ReadOne()
		if err != nil {
			break
		}
		if lines == 1 {
			nfields = len(rec)
		} else {
			if len(rec) != nfields {
				vv("too few fields(%v) on line %v, breaking out of read loop", len(rec), lines)
				break
			}
		}
		r = append(r, rec)
		lines++
	}

	vv("header = '%#v'", loader.Header)
	vv("1st line = '%#v'", r[0])
	df.Colnames = loader.Header
	df.Header = strings.Join(loader.Header, ",")

	var kind []ColumnKind

	// the set of columns that are numeric
	numeric := make(map[int]bool)

	// the set of columns that are factors
	factors := make(map[int]bool)

	// The factor vs numeric decision is made by
	// a test parse on the first row only, at the moment.
	for j, fld := range r[0] {
		_, err1 := strconv.ParseFloat(fld, 64)
		if err1 == nil {
			numeric[j] = true
			//vv("possibly numeric field: '%v' with example '%v'", loader.Header[j], fld)
			kind = append(kind, NUMERIC)
			df.numericNames = append(df.numericNames, loader.Header[j])
			df.numericCols = append(df.numericCols, j)
		} else {
			factors[j] = true
			//vv("is factor: '%v' with example '%v'", loader.Header[j], fld)
			kind = append(kind, FACTOR)
			df.HasFactors = true
			df.factorNames = append(df.factorNames, loader.Header[j])
			df.factorCols = append(df.factorCols, j)
		}
	}
	df.tradNumeric = numeric
	df.tradFactors = factors

	df.Kindvec = kind
	df.Ncol = len(r[0])

	nrow := lines - 1
	df.Nrow = nrow
	df.NumericMat = NewMatrix[float64](nrow, len(numeric))
	df.NumericMat.Colnames = df.numericNames

	df.FactorMat = NewMatrix[int](nrow, len(factors))
	df.FactorMat.Colnames = df.factorNames

	factorMaps := make([]map[string]int, len(factors))
	invFactorMaps := make([]map[int]string, len(factors))
	for j := range factorMaps {
		// factorMaps[j] is the map for the jth-column of df.FactorMat,
		// which maps field-value-strings to integer codes
		factorMaps[j] = make(map[string]int)

		// invFactorMaps[j] is the map for the jth-column of df.FactorMat,
		// which maps integer codes to field-value-strings
		invFactorMaps[j] = make(map[int]string)
	}
	df.factorMaps = factorMaps
	for i := range r {
		jnum := 0
		jfac := 0
		for j, fld := range r[i] {
			if kind[j] == NUMERIC {
				v, err := strconv.ParseFloat(fld, 64)
				panicOn(err)
				df.NumericMat.Set(i, jnum, v)
				jnum++
			} else {
				mp := factorMaps[jfac]
				code, ok := mp[fld]
				if !ok {
					// for now we encode based on order of encounter; we'll
					// lexically sort and recode below to match the python
					// reference impl.
					code = len(mp)
					mp[fld] = code
					inv := invFactorMaps[jfac]
					inv[code] = fld

					// we are using int now, not uint16. So do not panic.
					//if code > 65535 {
					//	panic(fmt.Sprintf("path '%v' on column %v ('%v') had more than 16-bits worth of factors -- this will be a problem for our uint16 factor encoding", path, j, df.factorNames[jfac]))
					//}
				}
				df.FactorMat.Set(i, jfac, code)
				jfac++
			}
		}
	}

	// re-encode factors based on lexical sort, to match pandas/python reference impl.
	for j := 0; j < df.FactorMat.Ncol; j++ {

		mp := factorMaps[j]

		var sortme LexCodeSlice
		for k, v := range mp {
			sortme = append(sortme, lexcode{lex: k, origCode: v})
		}
		sort.Sort(sortme)

		// derive the new forward and reverse maps from
		// the now lexically ordered sortme array
		mp1 := make(map[string]int)
		inv1 := make(map[int]string)

		// and how to convert the original encoding to the new
		old2new := make(map[int]int)

		// to match what dataframe.py does, injecting MISSINGLEVEL
		// at level 0, we'll add 1 to the new codes when we
		// re-encode, leaving the 0 spot for MISSINGLEVEL, aka "."

		levels := []string{MISSINGLEVEL}
		for i, v := range sortme {
			inv1[i+1] = v.lex
			mp1[v.lex] = i + 1
			old2new[v.origCode] = i + 1
			levels = append(levels, v.lex)
		}
		inv1[0] = MISSINGLEVEL
		mp1[MISSINGLEVEL] = 0

		// update the column's encoding
		for i := 0; i < df.FactorMat.Nrow; i++ {
			orig := df.FactorMat.At(i, j)
			df.FactorMat.Set(i, j, old2new[orig])
		}

		// and update the factorMaps, as their new encoding is now in use.
		factorMaps[j] = mp1
		invFactorMaps[j] = inv1

		df.FactorMat.Cmeta[j] = FeatMeta{
			Name:         df.FactorMat.Colnames[j],
			IsFactor:     true,
			Colj:         j,
			Levels:       levels,
			LevelCount:   len(levels),
			FactorMap:    mp1,
			InvFactorMap: inv1,
			MyMat:        df.FactorMat,
		}
	}

	if len(numeric) > 0 {
		df.Matrix = df.NumericMat.Dat // share data.
		df.Ncol = df.NumericMat.Ncol  // SlurpDataFrame will see only the numeric matrix; not the factor.
		// verify our assumption that df.NumericMat is still row major.
		if df.NumericMat.IsColMajor {
			panic("df.NumericMat should be row major for compatibility with SlurpDataFrame.Matrix expectations. We need to add a call to df.NumericMat.ReformatToRowMajor() here.")
		}

		// and set the FeatMeta for the NumericMat too, so if/when we
		// convert to factor, we have a name etc.
		for j := 0; j < df.NumericMat.Ncol; j++ {
			df.NumericMat.Cmeta[j] = FeatMeta{
				Name:     df.NumericMat.Colnames[j],
				IsFactor: false,
				Colj:     j,
				MyMat:    df.NumericMat,
			}

		}
	}
	return
}

func (df *SlurpDataFrame) Slurp(path string) (err error) {

	if strings.HasSuffix(path, ".gz") {
		// cannot memory map, must gunzip first.
		return df.ReadGzipped(path)
	}
	t0 := time.Now()

	// first argument is the path to the csv file to parse
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()
	df.Frompath = path

	newline := []byte("\n")

	// the whole file, memory mapped into a single []byte slice.
	buf := MemoryMapFile(fd)
	defer func(buf []byte) {
		err := syscall.Munmap(buf)
		panicOn(err)
	}(buf)

	// find the first newline, to delimit the header
	endHeader := bytes.Index(buf, newline)
	if endHeader < 0 {
		return ErrNoHeader
	}
	df.Header = string(buf[:endHeader])
	df.Colnames = strings.Split(df.Header, ",")
	df.Nheader = len(df.Colnames)

	// trailing blank lines can mess us up, deal with these up front,
	// by removing them, so bytes.Split() gives us only whole lines,
	// and not an empty last line.

	nbuf := len(buf)
	i := nbuf
	for ; i >= 1; i-- {
		if buf[i-1] != '\n' {
			break
		}
	}
	buf = buf[:i]

	byby := bytes.Split(buf, newline)

	bybyLines := len(byby)
	//vv("elap to bytes.Split into %v lines: %v\n", bybyLines, time.Since(t0))

	if bybyLines <= 1 {
		// only header? no data?
		//fmt.Printf("no lines after header detected\n")
		return ErrNoData
	}
	// INVAR: bybyLines >= 2

	// subtract off the header line, since not parsing it into float64.
	// fmt.Printf("we do not parse the header as float64. header line ='%v'\n", string(byby[0]))
	byby = byby[1:]
	df.Nrow = len(byby)

	df.Ncol = df.Nheader - 2 // leave off first two string fields.
	if df.Missing2strings {
		df.Ncol = df.Nheader // we have no initial strings, just float64 data
		df.MatrixColnames = df.Colnames
	} else {
		df.Tm = make([]time.Time, df.Nrow)
		df.MatrixColnames = df.Colnames[2:]
	}

	df.Matrix = make([]float64, df.Nrow*df.Ncol)

	if !df.Missing2strings {
		// pull the sym from the first data row
		comma := []byte(",")
		flds := bytes.Split(byby[0], comma)
		df.Sym = string(flds[1])
	}

	ngoro := 40

	each := df.Nrow / ngoro // integer divide does the floor automatically
	numBackground := each * ngoro
	left := df.Nrow - numBackground
	//vv("ngoro = %v; each = %v, numBackground = %v, left = %v, df.Nrow = %v\n", ngoro, each, numBackground, left, df.Nrow)

	var wg sync.WaitGroup
	waitCount := 0
	if each > 0 {
		waitCount = ngoro
	}
	if left > 0 {
		waitCount++
	}
	// do all the Adding before starting any goro,
	// or decrementing on the main goro with just a few left,
	// to avoid stopping prematurely. Per the docs.
	wg.Add(waitCount)

	if each > 0 {
		for i := 0; i < ngoro; i++ {
			beg := i * each
			endx := (i + 1) * each

			begM := beg * df.Ncol
			endxM := endx * df.Ncol

			go df.doChunk(&wg, byby[beg:endx], df.Matrix[begM:endxM], beg, endx)
		}
	}
	if left > 0 {
		// do any leftovers ourselves: usually less work.
		beg := numBackground
		endx := df.Nrow
		begM := beg * df.Ncol
		endxM := endx * df.Ncol
		df.doChunk(&wg, byby[beg:endx], df.Matrix[begM:endxM], beg, endx)
	}
	wg.Wait()

	vv("nCol = %v; nlines= %v ; float64 parsed k= %v; elap='%v'\n", df.Ncol, df.Nrow, df.parseTot, time.Since(t0))

	// make available via the NumericMat interface too; but without copying.
	// Just point at the same data.
	df.NumericMat = &Matrix[float64]{
		Nrow: df.Nrow,
		Ncol: df.Ncol,
		Dat:  df.Matrix,
	}
	df.NumericMat.Colnames = df.MatrixColnames

	return nil
}

// locate the row at or prior to tm. Can return -1 if tm is before us,
// or -2 if tm is after us. Checks within 1 minute or <= 2*si too, near
// the first and last, since that is a common case where the actual
// sample row will be close but maybe not exactly at the boundaries.
func (df *SlurpDataFrame) FindTm(tm time.Time, si time.Duration) (rowi int, err error) {

	if si < 0 {
		si = -si
	}

	// common case that samp -b is 1 sec, minute or within si of our row 0
	bdist := df.Tm[0].Sub(tm)
	if bdist < 0 {
		bdist = -bdist
	}
	if bdist <= time.Minute || bdist <= 2*si {
		vv("found first row")
		return 0, nil
	}

	// common case that samp -e is near our last row
	last := df.Nrow - 1
	edist := df.Tm[last].Sub(tm)
	if edist < 0 {
		edist = -edist
	}
	if edist <= time.Minute || edist <= 2*si {
		vv("found last row")
		return last, nil
	}

	//vv("df.Tm[0] = '%v' (%v) was not equal tm = '%v' (%v)", df.Tm[0], df.Tm[0].UnixNano(), tm, tm.UnixNano())

	// Search returns the smallest index i in [0, n) at which f(i) is true
	k := sort.Search(df.Nrow, func(i int) bool {
		return df.Tm[i].After(tm) // df.Tm[i] > tm
	})
	if k == df.Nrow {
		return -2, fmt.Errorf("could not FindTm(tm='%v'); it appears to be after all our tm. our data frame has '%v' to '%v'",
			tm.In(GTZ), df.Tm[0].In(GTZ), df.Tm[len(df.Tm)-1].In(GTZ))
	}
	if k == 0 {
		return -1, fmt.Errorf("could not FindTm(tm='%v'); it appears to be before all our tm. our data frame has '%v' to '%v'",
			tm.In(GTZ), df.Tm[0].In(GTZ), df.Tm[len(df.Tm)-1].In(GTZ))
	}
	return k - 1, nil
}

func (df *SlurpDataFrame) Row(i int) (tm time.Time, dat []float64) {
	k := i * df.Ncol
	return df.Tm[i], df.Matrix[k:(k + df.Ncol)]
}

// RowSlice can use nSpan to request just the past-looking predictors at the
// beginning of the row; i.e. without future targets which are typically at the end.
// nSpan must be >= 1, else the rowslice returned will be empty.
// The returned rowslice will be nSpan in length, being the row i sliced to [0:nSpan]
func (df *SlurpDataFrame) RowSlice(i int, nSpan int) (rowslice []float64) {
	if i < 0 || i >= df.Nrow {
		panic(fmt.Sprintf("RowSlice(i=%v) out of range of [0, nRow=%v)", i, df.Nrow))
	}
	if nSpan < 1 {
		return nil
	}
	k := i * df.Ncol
	return df.Matrix[k:(k + nSpan)]
}

func (df *SlurpDataFrame) doChunk(wg *sync.WaitGroup, byby [][]byte, matrix []float64, beg, endx int) {

	defer wg.Done()

	//vv("beg = %v, endx= %v\n", beg, endx)

	comma := []byte(",")
	k := 0 // which matrix element are we on.
	j := 0 // which tm,sym element are we on.
	var y float64
	var err error
	begField := 2
	if df.Missing2strings {
		begField = 0
	}

	for _, line := range byby {
		flds := bytes.Split(line, comma)
		//vv("j = %v, flds[0]='%v'; flds[1]='%v'", j, string(flds[0]), string(flds[1]))
		if !df.Missing2strings {
			df.Tm[beg+j], err = time.Parse(time.RFC3339Nano, *(*string)(unsafe.Pointer(&flds[0])))
			panicOn(err)
		}
		j++
		for i, by := range flds {
			if i >= begField {
				y, _ = strconv.ParseFloat(*(*string)(unsafe.Pointer(&by)), 64)
				matrix[k] = y
				k++
			}
		}
	}
	df.mut.Lock()
	df.parseTot += k
	df.mut.Unlock()
}

// memory mapped counting of newlines: very fast even on a single core,
// because it uses bytes.Count().
func CountLines(fd *os.File) (nline int, mmap []byte) {
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fd.Fd()), &stat)
	panicOn(err)

	sz := stat.Size
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ // | syscall.PROT_WRITE
	mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(sz), prot, flags)
	panicOn(err)

	newline := []byte("\n")

	nline = bytes.Count(mmap, newline)

	// add 1 if the last line is not terminated by newline
	// so that we account for needing to parse it as a line too.
	if mmap[len(mmap)-1] != '\n' {
		nline++
	}

	return
}

func MemoryMapFile(fd *os.File) (mmap []byte) {
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fd.Fd()), &stat)
	panicOn(err)
	sz := stat.Size
	vv("got sz = %v", sz)
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ
	mmap, err = syscall.Mmap(int(fd.Fd()), 0, int(sz), prot, flags)
	panicOn(err)
	return
}

// ExtractXXYY extracts a contiguous X range and one Y variable from sdf.
// See also ExtractCols if the X cols desired are not a continguous range.
//
// xi0 : row index of first X data
// xi1 : endx row to use (excluded)
//
// xj0 : first X column to use
// xj1 : endx X column to use (excluded)
//
// yj  : target Y column to use, just another column index into the same data frame (targets at the end)
//
// sdf : the data frame to grab the X and Y data from
//
// Note: we need to copy the X and Y data anyway, generaly.
//
// ///
func (sdf *SlurpDataFrame) ExtractXXYY(xi0, xi1, xj0, xj1, yj int) (n, nvar int, xx, yy []float64, colnames []string, targetname string) {

	if xj0 < 0 {
		panic(fmt.Sprintf("xj0(%v) cannot be negative", xj0))
	}
	if xj0 >= sdf.Ncol {
		panic(fmt.Sprintf("xj0(%v) must be < sdf.Ncol(%v)", xj0, sdf.Ncol))
	}
	if xj1 < 0 {
		panic(fmt.Sprintf("xj1(%v) cannot be negative", xj1))
	}
	if xj1 > sdf.Ncol {
		panic(fmt.Sprintf("xj1(%v) must be <= sdf.Ncol(%v)", xj1, sdf.Ncol))
	}
	if xi1 > sdf.Nrow {
		panic(fmt.Sprintf("ExtractXXYY() call error: xi1(%v) > sdf.Nrow(%v)", xi1, sdf.Nrow))
	}
	if xi0 >= sdf.Nrow {
		panic(fmt.Sprintf("ExtractXXYY() call error: xi0(%v) >= sdf.Nrow(%v)", xi0, sdf.Nrow))
	}
	if xi0 < 0 {
		panic("xi0 cannot be negative")
	}
	if xi1 <= 0 {
		panic("xi1 must be positive")
	}

	for j := xj0; j < xj1; j++ {
		colnames = append(colnames, sdf.MatrixColnames[j])
	}
	targetname = sdf.MatrixColnames[yj]

	n = xi1 - xi0
	nvar = xj1 - xj0

	xx = make([]float64, nvar*n)
	yy = make([]float64, n)

	for i := 0; i < n; i++ {
		row := sdf.RowSlice(xi0+i, xj1)
		copy(xx[i*nvar:], row[xj0:])
		yy[i] = sdf.MatrixAt(xi0+i, yj)
	}

	return
}

// ExtractCols extracts the wcol columns from sdf. The rows are from [xi0:xi1).
// See also ExtractXXYY if the X cols desired are a continguous range.
//
// xi0 : row index of first X data
// xi1 : the excluded endx index of a row that is just after the last included row
//
// n returns the number of rows back in xx;
// nvar returns the numer of variables back in xx;
// xx is the matrix (or vector if nvar == 1) of data extracted from sdf;
//
// ////
func (sdf *SlurpDataFrame) ExtractCols(xi0, xi1 int, wcol []int) (n, nvar int, xx []float64, colnames []string) {

	uniq := make(map[int]bool)
	for wj, j := range wcol {
		_, already := uniq[j]
		if already {
			panic(fmt.Sprintf("duplicate wcol entry found for j=%v at pos %v", j, wj))
		}
		uniq[j] = true

		if j < 0 {
			panic(fmt.Sprintf("j(%v) at pos %v cannot be negative", j, wj))
		}
		if j >= sdf.Ncol {
			panic(fmt.Sprintf("j(%v) at pos %v must be < sdf.Ncol(%v)", j, wj, sdf.Ncol))
		}
	}
	if xi1 > sdf.Nrow {
		panic(fmt.Sprintf("ExtractCols() call error: xi1(%v) > sdf.Nrow(%v)", xi1, sdf.Nrow))
	}
	if xi0 >= sdf.Nrow {
		panic(fmt.Sprintf("ExtractCols() call error: xi0(%v) >= sdf.Nrow(%v)", xi0, sdf.Nrow))
	}
	if xi0 < 0 {
		panic("xi0 cannot be negative")
	}
	if xi1 <= 0 {
		panic("xi1 must be positive")
	}

	n = xi1 - xi0
	nvar = len(wcol)

	xx = make([]float64, nvar*n)

	for _, j := range wcol {
		colnames = append(colnames, sdf.MatrixColnames[j])
	}

	for i := 0; i < n; i++ {
		for wj, j := range wcol {
			xx[i*nvar+wj] = sdf.MatrixAt(xi0+i, j)
		}
	}

	return
}

// LexCode is for sorting factors lexically and re-coding them (to match pandas)
// lexcode holds a factor string value, the corresponding original
// factor integer code.
type lexcode struct {
	lex      string
	origCode int
}

func (v *lexcode) String() (r string) {
	return fmt.Sprintf("&lexcode{lex:\"%v\", origCode:%v}", v.lex, v.origCode)
}

// LexCodeSlice facilitates sorting by factor name lexically
type LexCodeSlice []lexcode

func (p LexCodeSlice) Len() int { return len(p) }
func (p LexCodeSlice) Less(i, j int) bool {
	return p[i].lex < p[j].lex
}
func (p LexCodeSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p LexCodeSlice) String() (r string) {
	r = "LexCodeSlice{\n"
	for i := range p {
		r += fmt.Sprintf("%v,\n", p[i])
	}
	r += "}"
	return
}
