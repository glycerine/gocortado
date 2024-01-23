package gocortado

type Seq[S any, T any] struct {
	genf GenFunc[S, T]
}

type NextFunc[S any, T any] func(state S) (value T, ok bool, newstate S)

type GenFunc[S any, T any] func() (state S, next NextFunc[S, T])

func SeqFromNext[S any, T any](state S, next NextFunc[S, T]) Seq[S, T] {
	f := func() (S, NextFunc[S, T]) {
		return state, next
	}
	return Seq[S, T]{genf: f}
}

func SeqMap[S any, T any, U any](f func(T) U, xs Seq[S, T]) Seq[S, U] {
	var state, next = xs.genf()
	var newnext = func(s S) (U, bool, S) {
		var v, ok, newstate = next(s)
		if !ok {
			return *new(U), ok, s
		} else {
			return f(v), ok, newstate
		}
	}
	return SeqFromNext[S, U](state, newnext)
}

type SlicingState struct {
	start       int
	length      int
	sliceLength int
}

type SlicePosition struct {
	start int
	end   int
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func NextSlicePosition(state SlicingState) (SlicePosition, bool, SlicingState) {
	sliceLen := min(state.length, state.sliceLength)
	newstart := state.start + sliceLen
	newlen := state.length - sliceLen
	if sliceLen <= 0 {
		return SlicePosition{start: 0, end: 0}, false, state
	} else {
		return SlicePosition{
				start: state.start,
				end:   state.start + sliceLen,
			},
			true,
			SlicingState{
				start:       newstart,
				length:      newlen,
				sliceLength: sliceLen,
			}
	}
}
