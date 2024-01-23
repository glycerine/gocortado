package gocortado

import (
	"fmt"
	"runtime/pprof"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test004_learn_x3_x4_xor_synthetic_data(t *testing.T) {

	cv.Convey("In a synthetic data set of ten standard normal variables, 4000 rows sampled, where the target is the xor of the signs of x3 and x4, the booster should be able pick out features x3 and x4 and predict nearly perfectly", t, func() {

		fn := "data/xor_synth_x3x4_predict.csv"
		df := NewSlurpDataFrame(true)
		t0 := time.Now()
		err := df.Slurp(fn)
		panicOn(err)
		vv("slurped in fn '%v' in %v", fn, time.Since(t0))

		nr := df.Nrow // Number of cases in full database
		nc := df.Ncol // Number of columns (X variables and Y target)
		vv("we see nc = %v, nr= %v", nc, nr)

		//mat := NewMatrix[float64](nr, nc)
		//mat.FillRowMajor(df.Matrix, false)

		//label := make([]float64, nr)

		wcol := []int{10}
		xi0 := 0
		xi1 := df.Nrow
		n, nvar, target, cny := df.ExtractCols(xi0, xi1, wcol)
		_ = n
		_ = nvar
		_ = cny

		vv("target head = '%#v'", target[:100])

		//label = df["dep_delayed_15min"].map({"N": 0, "Y": 1})

		//covariates := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

		// exclude the target from the X predictors to learn on.
		df.NumericMat.DeleteCols([]int{6})

		cov2fac := FactorFromCovarMatrix(df.NumericMat)

		// avoid our panic warning about taking a RowChunk from a column-major matrix.
		cov2fac.ReformatToRowMajor()

		vv("cov2fac head = '%v'", cov2fac.RowChunk(0, 10))

		factors := cov2fac

		eta := 0.1
		nrounds := 1
		max_depth := 1
		lambda_ := 1.0
		gamma := 0.0
		minh := 1.0
		mu := 0.5
		var trainselector []bool
		slicelen := 4000

		start := time.Now()
		trees, predictions := xgblogit(target, factors, trainselector, mu, eta, lambda_,
			gamma, max_depth, nrounds, minh, slicelen)
		elap := time.Since(start)
		vv("gocortado elapsed: %v", elap)
		_ = trees
		//y := target
		//auc := roc_auc_score(y, pred)
		aucroc, err := AreaUnderROC(predictions, target, []*TargetRange{&TargetRange{Thresh: 0.5}})
		panicOn(err)
		vv("gocortado auc = %v", aucroc[0].Auc1)
		cv.So(aucroc[0].Auc1, cv.ShouldEqual, 1)
	})
}

// "airlinetrain1m.csv"
func Test005_airline_data_test(t *testing.T) {

	cv.Convey("use the data/airlinetrain1m.csv.gz data set and reproduce the tests/test_xgblogit.py test", t, func() {

		fn := "data/airlinetrain1m.csv.gz"
		df := NewSlurpDataFrame(true)
		t0 := time.Now()
		err := df.Slurp(fn)
		panicOn(err)
		vv("slurped in fn '%v' in %v", fn, time.Since(t0))

		nr := df.Nrow // Number of cases in full database
		nc := df.Ncol // Number of columns (X variables and Y target)
		vv("we see nc = %v, nr= %v; df.HasFactors=%v", nc, nr, df.HasFactors)

		vv("df.NumericMat.Colnames = '%#v' nrow=%v, ncol=%v; head =\n%v\n", df.NumericMat.Colnames, df.NumericMat.Nrow, df.NumericMat.Ncol, df.NumericMat.RowChunk(0, 10))
		vv("df.FactorMat = '%#v' nrow=%v, ncol=%v; head = \n%v\n", df.FactorMat.Colnames, df.FactorMat.Nrow, df.FactorMat.Ncol, df.FactorMat.RowChunk(0, 10))

		targetFactorInt := df.FactorMat.Col(6)

		ycn := df.FactorMat.Colnames[6]
		if ycn != "dep_delayed_15min" {
			panic(fmt.Sprintf("wrong target column: '%v'; we wanted 'dep_delayed_15min'", ycn))
		}
		_ = targetFactorInt

		// convert the target from uint16 {1,2} (default factor for Y/N values) to float64 {0,1}.
		// the 0 for MISSINGLEVEL will have increased the {0,1} to {1,2} on us.
		target := make([]float64, len(targetFactorInt))
		for i := range target {
			target[i] = float64(targetFactorInt[i] - 1)
			if target[i] < 0 {
				panic("we should not have any missing data in the target!")
			}
		}
		_ = target
		vv("target = '%#v'...", target[:20])

		// then delete the target from the factor matrix
		df.FactorMat.DeleteCols([]int{6})

		vv("df after DeleteCols([]int{6}) = '%v'", df.FactorMat.RowChunk(0, 10))

		// like test_xgblogit.py calling
		//
		// deptime = Factor.from_covariate(train_df["DepTime"]).cached()
		//
		// we will next convert deptime (and distance) from covariates to factors,
		// so that we have all factors; the starting cortado code only deals
		// in factors. We will implement other variants after the factor-only is working.

		cov2fac := FactorFromCovarMatrix(df.NumericMat)

		// avoid our panic warning about taking a RowChunk from a column-major matrix.
		//cov2fac.ReformatToRowMajor()

		vv("cov2fac = '%v'", cov2fac) // .RowChunk(0, 10))

		// append the new covariate factor matrix columns on to the right side of df.FactorMat
		df.FactorMat.Cbind(cov2fac)

		vv("df after Cbind = '%v'", df.FactorMat) // .RowChunk(0, 10))

		// okay, so we have our full data at the ready, in df.FactorMat

		// does it look okay?
		//df.FactorMat.ReformatToRowMajor()
		//vv("df.FactorMat after Cbind() = '%v'", df.FactorMat) // .RowChunk(0, 10))

		//df.FactorMat.ReformatToSliceOfColVec()

		var trainselector []bool
		mu := 0.5

		lambda_ := 1.0
		gamma := 0.0
		nrounds := 100
		//nrounds := 1
		eta := 0.1
		max_depth := 8
		slicelen := 10000
		//slicelen := 5
		minh := 1.0

		// turn off profiling, for now, because it slows down the run by a little.
		if false {
			startProfiling(true, "cpu_profile_out", "mem_profile_out")
			defer pprof.StopCPUProfile()
		}

		//t0train := time.Now()

		trees, predictions := xgblogit(target, df.FactorMat, trainselector, mu, eta, lambda_, gamma, max_depth, nrounds, minh, slicelen)
		_ = trees
		_ = predictions

		//vv("time to train and predict on training set = '%v' ; nrounds=%v, max_depth=%v", time.Since(t0train), nrounds, max_depth)

		//vv("predictions(len %v; first 100 here) = '%#v'", len(predictions), predictions[:100])

		if false {
			pred1count := 0
			targ1count := 0
			for i, pr := range predictions {
				if pr > 0.5 {
					pred1count++
				}
				if target[i] > 0.5 {
					targ1count++
				}
			}
			vv("pred1count = %v ; targ1count = %v", pred1count, targ1count)
		}

		//y := target
		//auc := roc_auc_score(y, pred)
		aucroc, err := AreaUnderROC(predictions, target, []*TargetRange{&TargetRange{Thresh: 0.5}})
		panicOn(err)
		vv("gocortado auc = %v", aucroc[0].Auc1)
		cv.So(aucroc[0].Auc1, cv.ShouldEqual, 0.7772975612579787)

	})
}

// "data/a.over.10.target.csv"
func Test006_a_over_10_target(t *testing.T) {

	cv.Convey("use the data/a.over.10.target.csv as simple 20 row data set. a > 10 is a perfect predictor of target class 1. The other 4 columns are random uniformly sampled integer factors from [1,20]. We should obtain an AUROC of 1.0", t, func() {

		path := "data/a.over.10.target.csv"
		df := NewSlurpDataFrame(true)
		t0 := time.Now()
		err := df.Slurp(path)
		panicOn(err)
		vv("slurped in fn '%v' in %v", path, time.Since(t0))

		nr := df.Nrow // Number of cases in full database
		nc := df.Ncol // Number of columns (X variables and Y target)
		vv("we see nc = %v, nr= %v; df.HasFactors=%v", nc, nr, df.HasFactors)

		vv("df.NumericMat.Colnames = '%#v' nrow=%v, ncol=%v; head =\n%v\n", df.NumericMat.Colnames, df.NumericMat.Nrow, df.NumericMat.Ncol, df.NumericMat)

		target := df.NumericMat.Col(5)
		vv("target = '%#v'", target)

		ycn := df.NumericMat.Colnames[5]
		if ycn != "target" {
			panic(fmt.Sprintf("wrong target column: '%v'; we wanted 'target'", ycn))
		}

		// then delete the target from the numeric matrix
		df.NumericMat.DeleteCols([]int{5})

		cov2fac := FactorFromCovarMatrix(df.NumericMat)

		// avoid our panic warning about taking a RowChunk from a column-major matrix.
		cov2fac.ReformatToRowMajor()

		vv("cov2fac = '%v'", cov2fac)

		var trainselector []bool
		mu := 0.5

		lambda_ := 1.0
		gamma := 0.0
		//nrounds := 100
		nrounds := 1
		eta := 0.1
		max_depth := 1
		//slicelen := 20 // gets it right all in one batch
		slicelen := 1 // gets it wrong, predicting all true
		//slicelen := 10 // more right, fewer wrong, but not 100% correct like can be acheived.
		minh := 1.0

		t0train := time.Now()

		trees, predictions := xgblogit(target, cov2fac, trainselector, mu, eta, lambda_, gamma, max_depth, nrounds, minh, slicelen)
		_ = trees
		_ = predictions

		vv("time to train and predict on training set = '%v' ; nrounds=%v, max_depth=%v", time.Since(t0train), nrounds, max_depth)

		hardpred := make([]float64, len(predictions))
		for i, pi := range predictions {
			if pi > 0.5 {
				hardpred[i] = 1
			} else {
				hardpred[i] = 0
			}
		}

		vv("target   = '%#v'", target)
		vv("hardpred = '%#v'", hardpred)
		vv("predictions(len %v) = '%#v'", len(predictions), predictions)

		pred1count := 0
		targ1count := 0
		var tp, fp, tn, fn int
		for i, pr := range predictions {
			if pr > 0.5 {
				pred1count++
				if target[i] > 0.5 {
					tp++
				} else {
					fp++
				}
			} else {
				if target[i] > 0.5 {
					fn++
				} else {
					tn++
				}
			}
			if target[i] > 0.5 {
				targ1count++
			}
		}
		vv("pred1count = %v ; targ1count = %v", pred1count, targ1count)
		vv("truePos = %v, trueNeg = %v, falsePos = %v, falseNeg = %v", tp, tn, fp, fn)

		cv.So(pred1count, cv.ShouldEqual, 10)
		cv.So(tp, cv.ShouldEqual, 10)
		cv.So(tn, cv.ShouldEqual, 10)

		//y := target
		//auc := roc_auc_score(y, pred)
		aucroc, err := AreaUnderROC(predictions, target, []*TargetRange{&TargetRange{Thresh: 0.5}})
		panicOn(err)
		vv("gocortado auc = %v", aucroc[0].Auc1)
		cv.So(aucroc[0].Auc1, cv.ShouldEqual, 1.0)
	})
}
