package gocortado

import (
	"fmt"
	"math"
	"sort"
)

type CutCovFactor struct {
	Covariate []float64
	Factor    []int

	Meta FeatMeta
}

func g_leftclosed(slice []float64, cuts []float64, k int) (factor []int) {

	f := func(x float64) int {

		if math.IsNaN(x) {
			return 0
		}
		if math.IsInf(x, 1) {
			return k
		} else {
			// np.searchsorted(cuts, x, 'right'); where the
			// 'right' means np.searchsorted returns i: a[i-1] <= v < a[i]
			// and
			// 'left' means i: a[i-1] < v <= a[i]
			// If there is no suitable index, return either 0 or N (where N is the length of a).
			// -- https://numpy.org/doc/stable/reference/generated/numpy.searchsorted.html
			//
			// Go standard lib doc for sort.Search():
			// sort.Search uses binary search to find and return
			// the smallest index i in [0, n) at which f(i)
			// is true, assuming that on the range [0, n),
			// f(i) == true implies f(i+1) == true. That is,
			// Search requires that f is false for some
			// (possibly empty) prefix of the input range [0, n)
			// and then true for the (possibly empty) remainder;
			// Search returns the first true index. If there is
			// no such index, Search returns n. (Note that the
			// "not found" return value is not -1 as in, for
			// instance, strings.Index.) Search calls f(i) only
			// for i in the range [0, n).
			// -- https://pkg.go.dev/sort#Search
			//
			return sort.Search(len(cuts), func(i int) bool {
				return x < cuts[i]
			})
		}
	}

	factor = make([]int, len(slice))
	for i, v := range slice {
		factor[i] = f(v)
	}
	return
}

func g_rightclosed(slice []float64, cuts []float64) (factor []int) {

	f := func(x float64) int {
		if math.IsNaN(x) {
			return 0
		}
		if math.IsInf(x, -1) {
			return 1
		}
		// i = np.searchsorted(cuts, x, side='left')
		// return i

		// 'left' means i: a[i-1] < v <= a[i]
		// If there is no suitable index, return either 0 or N (where N is the length of a).
		// -- https://numpy.org/doc/stable/reference/generated/numpy.searchsorted.html
		//
		// Go standard lib doc for sort.Search():
		// sort.Search uses binary search to find and return
		// the smallest index i in [0, n) at which f(i)
		// is true, assuming that on the range [0, n),
		// f(i) == true implies f(i+1) == true. That is,
		// Search requires that f is false for some
		// (possibly empty) prefix of the input range [0, n)
		// and then true for the (possibly empty) remainder;
		// Search returns the first true index. If there is
		// no such index, Search returns n. (Note that the
		// "not found" return value is not -1 as in, for
		// instance, strings.Index.) Search calls f(i) only
		// for i in the range [0, n).
		// -- https://pkg.go.dev/sort#Search
		//
		return sort.Search(len(cuts), func(i int) bool {
			return x <= cuts[i]
		})
	}
	factor = make([]int, len(slice))
	for i, v := range slice {
		factor[i] = f(v)
	}
	return
}

func (f *CutCovFactor) SetLevels() {

	levels := []string{}
	cuts := f.Meta.Cuts

	if f.Meta.Rightclosed {
		for i := 0; i < f.Meta.LevelCount; i++ {
			var lev string
			if i == 0 {
				lev = fmt.Sprintf("[%v,%v]", cuts[i], cuts[i+1])
				levels = append(levels, lev)
			} else {
				lev = fmt.Sprintf("(%v,%v]", cuts[i], cuts[i+1])
				levels = append(levels, lev)
			}
			f.Meta.FactorMap[lev] = i
			f.Meta.InvFactorMap[i] = lev
		}
		// levels = [MISSINGLEVEL] + ["{z}{x},{y}]".format(x=str(cuts[i]), y=str(cuts[i + 1]), z="[" if i == 0 else "(") for i in range(levelcount)]
	} else {
		//levels = [MISSINGLEVEL] + ["[{x},{y}{z}".format(x=str(cuts[i]), y=str(cuts[i + 1]), z="]" if i == (levelcount - 1) else ")") for i in range(levelcount)]

		for i := 0; i < f.Meta.LevelCount; i++ {
			var lev string
			if i == f.Meta.LevelCount-1 {
				lev = fmt.Sprintf("[%v,%v]", cuts[i], cuts[i+1])
				levels = append(levels, lev)
			} else {
				lev = fmt.Sprintf("[%v,%v)", cuts[i], cuts[i+1])
				levels = append(levels, lev)
			}
			f.Meta.FactorMap[lev] = i
			f.Meta.InvFactorMap[i] = lev
		}
	}

	// will this not mess up LevelCount != len(f.Levels) ???
	// well, we'll panic if we do not add MISSINGLEVEL... in split.go:58 f_hist()
	levels = append([]string{MISSINGLEVEL}, levels...)
	f.Meta.Levels = levels
}

func NewCutCovFactor(colname string, colj int, covariate, cuts []float64, rightclosed bool, myMat *Matrix[int]) (r *CutCovFactor) {
	r = &CutCovFactor{
		Covariate: covariate,
		Meta:      *NewFeatMeta(),
	}
	r.Meta.Name = colname
	r.Meta.Colj = colj
	r.Meta.IsOrdinal = true
	r.Meta.Rightclosed = rightclosed
	r.Meta.Cuts = cuts
	r.Meta.MyMat = myMat

	if !math.IsInf(cuts[0], -1) {
		panic(fmt.Sprintf("expected -Inf at cuts[0], but got '%v'", cuts[0]))
	}
	last := cuts[len(cuts)-1]
	if !math.IsInf(last, 1) {
		panic(fmt.Sprintf("expected +Inf at last cuts[%v], but got '%v'", len(cuts)-1, last))
	}

	r.Meta.LevelCount = len(cuts) - 1

	if rightclosed {
		r.Factor = g_rightclosed(covariate, cuts)
	} else {
		r.Factor = g_leftclosed(covariate, cuts, r.Meta.LevelCount-1)
	}
	r.SetLevels()

	return
}
