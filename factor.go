package gocortado

import (
	"math"
	"sort"
)

func FactorFromCovarMatrix(real *Matrix[float64]) (fac *Matrix[int]) {
	rightclosed := true
	fac = NewMatrix[int](real.Nrow, real.Ncol)
	fac.IsColMajor = true
	fac.Colnames = real.Colnames
	cuts := []float64{}

	for j := 0; j < real.Ncol; j++ {
		ccf := FactorFromCovariate(j, real.Colnames[j], real.Col(j), cuts, rightclosed, fac)
		fac.WriteCol(j, ccf.Factor)
		fac.Cmeta[j] = ccf.Meta
		fac.Cmeta[j].Colj = j
		fac.Cmeta[j].MyMat = fac
	}

	return
}

func FactorFromCovariate(colj int, covname string, covariate []float64, cuts []float64, rightclosed bool, myMat *Matrix[int]) *CutCovFactor {
	if len(cuts) > 0 {
		if !math.IsInf(cuts[0], -1) {
			cuts = append([]float64{math.Inf(-1)}, cuts...)
		}
		if !math.IsInf(cuts[len(cuts)-1], 1) {
			cuts = append(cuts, math.Inf(1))
		}
		return NewCutCovFactor(covname, colj, covariate, cuts, rightclosed, myMat)
	}

	// no cuts supplied! yikes! do our best...

	// This is a crude binning strategy that gives each real number present in the data its own
	// bin. Obviously this won't work for actual large data sets, but it is a simple starting point,
	// especially if the cardinality of the column is not too large.

	// TODO: determine bins by slicing the range of the covariate, possibly respecting
	// the density of the points by being finer grained at higher density. Then
	// pass in the cuts slice so we don't have to fallback to this
	// one-bin-per-real-number-in-the-data-set strategy.

	AlwaysPrintf("ALERT! no cuts supplied for '%v', falling back on crude one-bin-per-real-number...", covname)

	uniqifier := make(map[float64]bool)
	for _, x := range covariate {
		uniqifier[float64(x)] = true
	}
	var uniq []float64
	for k := range uniqifier {
		uniq = append(uniq, k)
	}
	//sort.Sort(Float32Slice(uniq))
	sort.Float64s(uniq)

	n := len(uniq)
	vv("have len(uniq) = %v factor levels for '%v'", n, covname)

	cuts = make([]float64, n+1)
	cuts[0] = math.Inf(-1)
	for i := 0; i < n; i++ {
		if i == n-1 {
			cuts[i+1] = math.Inf(1)
		} else {
			cuts[i+1] = uniq[i] + 0.5*(uniq[i+1]-uniq[i])
		}
	}
	return NewCutCovFactor(covname, colj, covariate, cuts, rightclosed, myMat)
}

// Vestigial from when we were thinking of doing float32. At the moment we
// are all float64, but save in case we want to try the float32.

// standard stuff to allow sort.Sort() to sort a slice.
type Float32Slice []float32

func (p Float32Slice) Len() int { return len(p) }

func (p Float32Slice) Less(i, j int) bool {
	if math.IsNaN(float64(p[i])) {
		// sort NaN to the back
		return false
	}
	if math.IsNaN(float64(p[j])) {
		// sort NaN to the back
		return true
	}
	return p[i] < p[j]
}

func (p Float32Slice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
