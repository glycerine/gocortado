package gocortado

import (
	//"fmt"
	//"math"
	//"os"
	"strings"
	"testing"
	"time"

	cv "github.com/glycerine/goconvey/convey"
)

func Test001_slurp_in_data(t *testing.T) {

	cv.Convey("read a .csv dataframe from disk into memory using all cores", t, func() {

		fn := "data/test001.csv"
		d := NewSlurpDataFrame(true)
		t0 := time.Now()
		err := d.Slurp(fn)
		panicOn(err)
		vv("slurped in fn '%v' in %v", fn, time.Since(t0))

		nr := d.Nrow // Number of cases in full database
		nc := d.Ncol // Number of columns (X variables and Y target)
		vv("we see nc = %v, nr= %v", nc, nr)

		// illustrate how to use the testing framework
		cv.So(nc, cv.ShouldEqual, 4)
		cv.So(nr, cv.ShouldEqual, 5)

		// expected
		eh := []string{"x1", "x2", "x3", "y"}
		em := [][]float64{
			[]float64{1.49152459063627, 2.49152289572389, 1.67045378357525, 1},
			[]float64{0.391160302666239, 1.39115885339298, 3.55649293629879, 1},
			[]float64{0.434211270774665, 1.43420995498692, 3.30302332417163, 0},
			[]float64{0.136364617767486, 1.13636348140514, 8.33327222273116, 0},
			[]float64{1.136364617767486, -3.13636348140514, -9.273116, 0},
		}
		_ = em
		cv.So(d.Header, cv.ShouldResemble, strings.Join(eh, ","))
		cv.So(d.Colnames, cv.ShouldResemble, eh)
		for i := range em {
			cv.So(d.MatFullRow(i), cv.ShouldResemble, em[i])
		}

		// ExtractCols
		xi0 := 3
		xi1 := 5
		wcol := []int{1, 3}
		n, nvar, xx, cn := d.ExtractCols(xi0, xi1, wcol)
		cv.So(n == 2, cv.ShouldBeTrue)
		cv.So(nvar == 2, cv.ShouldBeTrue)
		cv.So(cn, cv.ShouldResemble, []string{"x2", "y"})
		cv.So(xx[0], cv.ShouldResemble, em[3][1])
		cv.So(xx[1], cv.ShouldResemble, em[3][3])
		cv.So(xx[2], cv.ShouldResemble, em[4][1])
		cv.So(xx[3], cv.ShouldResemble, em[4][3])

		// ExtractXXYY
		xi0 = 4
		xi1 = 5  // just the last row
		xj0 := 2 // just the 3rd col
		xj1 := 3
		yj := 3 // and the target
		n, nvar, xx, yy, colnames, targetname := d.ExtractXXYY(xi0, xi1, xj0, xj1, yj)
		cv.So(n == 1, cv.ShouldBeTrue)
		cv.So(nvar == 1, cv.ShouldBeTrue)
		cv.So(colnames, cv.ShouldResemble, []string{"x3"})
		cv.So(targetname, cv.ShouldResemble, "y")
		cv.So(xx, cv.ShouldResemble, em[xi0][xj0:xj1])
		cv.So(yy, cv.ShouldResemble, em[xi0][yj:(yj+1)])

	})
}
