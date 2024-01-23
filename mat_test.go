package gocortado

import (
	"testing"

	cv "github.com/glycerine/goconvey/convey"
)

func Test003_Matrix_basic_tests(t *testing.T) {

	cv.Convey("Matrix should act like a mathematical matrix", t, func() {
		m := NewMatrix[int](2, 3)
		m.FillRowMajor([]int{0, 1, 2, 3, 4, 5}, false)
		m.Rownames = []string{"joe", "bob"}
		m.Colnames = []string{"x1", "x2", "y"}
		ms := m.String()
		//vv("m = '%v'", ms)
		cv.So(ms, cv.ShouldEqual, `Matrix[int](Nrow=2 x Ncol=3) = [
"rowid", "x1", "x2", "y"
"joe", 0, 1, 2
"bob", 3, 4, 5
]
`)
		cv.So(len(m.Cmeta), cv.ShouldEqual, 3)
		for j := 0; j < 3; j++ {
			cv.So(m.Cmeta[j].Colj, cv.ShouldEqual, j)
		}

		cv.So(m.Row(0), cv.ShouldResemble, []int{0, 1, 2})
		cv.So(m.Row(1), cv.ShouldResemble, []int{3, 4, 5})

		cv.So(m.Col(0), cv.ShouldResemble, []int{0, 3})
		cv.So(m.Col(1), cv.ShouldResemble, []int{1, 4})
		cv.So(m.Col(2), cv.ShouldResemble, []int{2, 5})

		m.Transpose()
		cv.So(m.Ncol, cv.ShouldEqual, 2)
		cv.So(m.Nrow, cv.ShouldEqual, 3)
		cv.So(m.Col(0), cv.ShouldResemble, []int{0, 1, 2})
		cv.So(m.Col(1), cv.ShouldResemble, []int{3, 4, 5})

		m.ReformatToRowMajor()
		cv.So(m.Row(0), cv.ShouldResemble, []int{0, 3})
		cv.So(m.Row(1), cv.ShouldResemble, []int{1, 4})
		cv.So(m.Row(2), cv.ShouldResemble, []int{2, 5})

		newi := m.AddRow("y2")
		cv.So(newi, cv.ShouldEqual, 3)
		cv.So(m.String(), cv.ShouldEqual, `Matrix[int](Nrow=4 x Ncol=2) = [
"rowid", "joe", "bob"
"x1", 0, 3
"x2", 1, 4
"y", 2, 5
"y2", 0, 0
]
`)
		m.Transpose()
		newi = m.AddRow("jane")
		cv.So(newi, cv.ShouldEqual, 2)
		cv.So(m.String(), cv.ShouldEqual, `Matrix[int](Nrow=3 x Ncol=4) = [
"rowid", "x1", "x2", "y", "y2"
"joe", 0, 1, 2, 0
"bob", 3, 4, 5, 0
"jane", 0, 0, 0, 0
]
`)
		cv.So(m.IsColMajor, cv.ShouldBeTrue)
		m.ReformatToColumnMajor()
		cv.So(m.String(), cv.ShouldEqual, `Matrix[int](Nrow=3 x Ncol=4) = [
"rowid", "x1", "x2", "y", "y2"
"joe", 0, 1, 2, 0
"bob", 3, 4, 5, 0
"jane", 0, 0, 0, 0
]
`)
		m.ReformatToRowMajor()
		ri := m.GetRowIter(1, 2, 1)
		row, done := ri.Fetch()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{3, 4, 5, 0})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{3, 4, 5, 0})

		row, done = ri.Fetch()
		cv.So(done, cv.ShouldBeTrue)
		cv.So(row == nil, cv.ShouldBeTrue)

		ri = m.GetRowIter(0, 3, 1)
		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{0, 1, 2, 0})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{3, 4, 5, 0})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{0, 0, 0, 0})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeTrue)
		cv.So(row == nil, cv.ShouldBeTrue)

		// row iterator on df with single column, as stand in for a simple slice []float64
		tot := m.Nrow * m.Ncol
		//vv("tot = %v", tot)

		m.Reshape(tot, 1)
		//m.Ncol = 1
		//m.Nrow = tot

		//vv("before m.Dat = '%#v'", m.Dat)
		for i := 0; i < tot; i++ {
			m.Dat[i] = i
		}
		//vv("after m.Dat = '%#v'", m.Dat)
		m.Rownames = nil
		m.Colnames = nil
		//vv("m = '%v'", m.String())

		ri = m.GetRowIter(0, tot, 3)
		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{0, 1, 2})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{3, 4, 5})

		row, done = ri.FetchAdv()
		//vv("row = '%#v'", row)
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{6, 7, 8})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeFalse)
		cv.So(row.Dat, cv.ShouldResemble, []int{9, 10, 11})

		row, done = ri.FetchAdv()
		cv.So(done, cv.ShouldBeTrue)
		cv.So(row == nil, cv.ShouldBeTrue)

		meta0 := m.CmetaDisplay()
		_ = meta0
		// m is 12 x 1 (nrow=12, ncol=1)
		//vv("m.Ncol = %v; m.Nrow = %v; meta0(len %v)='%v'", m.Ncol, m.Nrow, len(meta0), meta0)

		//m.Ncol = 4
		//m.Nrow = 3
		m.Reshape(3, 4)

		// verify that DeleteCols works on either format.
		meta1 := m.CmetaDisplay()
		//vv("m.CmetaDisplay() = '%#v'", meta1)
		cv.So(len(meta1), cv.ShouldEqual, m.Ncol)

		m2 := m.Clone()
		meta2 := m2.CmetaDisplay()
		//vv("m2.CmetaDisplay() = '%#v'", m2.CmetaDisplay())
		cv.So(meta2, cv.ShouldResemble, meta1)

		cv.So(m2.IsColMajor, cv.ShouldBeFalse)
		m2.ReformatToColumnMajor()
		//vv("before = '%v'", m)
		cv.So(m.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=4) = [
 0, 1, 2, 3
 4, 5, 6, 7
 8, 9, 10, 11
]
`)

		//vv("after ReformatToColumnMajor: m2.CmetaDisplay = '%#v'", m2.CmetaDisplay())
		//vv("after ReformatToColumnMajor: m.CmetaDisplay = '%#v'", m.CmetaDisplay())

		m.DeleteCols([]int{1, 3})
		//vv("after = '%v'", m)
		cv.So(m.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=2) = [
 0, 2
 4, 6
 8, 10
]
`)

		cv.So(m2.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=4) = [
 0, 1, 2, 3
 4, 5, 6, 7
 8, 9, 10, 11
]
`)
		m2.DeleteCols([]int{1, 3})
		//vv("after = '%v'", m)
		cv.So(m2.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=2) = [
 0, 2
 4, 6
 8, 10
]
`)
		m.Cbind(m2)
		cv.So(m.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=4) = [
 0, 2, 0, 2
 4, 6, 4, 6
 8, 10, 8, 10
]
`)
		m2.ReformatToRowMajor()
		m2.Set(0, 1, 35)
		m.Cbind(m2)
		cv.So(m.Ncol, cv.ShouldEqual, 6)
		cv.So(m.Nrow, cv.ShouldEqual, 3)
		cv.So(m.String(), cv.ShouldResemble, `Matrix[int](Nrow=3 x Ncol=6) = [
 0, 2, 0, 2, 0, 35
 4, 6, 4, 6, 4, 6
 8, 10, 8, 10, 8, 10
]
`)

		m3 := m.ExtractRowsColsAsMatrix(1, 3, []int{0, 2, 5}) // index out of range [0] with length 0
		cv.So(m3.String(), cv.ShouldResemble, `Matrix[int](Nrow=2 x Ncol=3) = [
 4, 4, 6
 8, 8, 10
]
`)

	})
}
