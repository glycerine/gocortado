package gocortado

import (
	"fmt"
)

func NewLevelPartitionAllTrue(n int) []bool {
	mask := make([]bool, n)
	for i := range mask {
		mask[i] = true
	}
	return mask
}

type Node struct {
	IsLeaf bool // else issplit

	// leaf attributes
	Gsum float64
	Hsum float64
	//GHsum    []float64 // []float64{rightgsum, righthsum} for instance; always 2 long... same as Gsum, Hsum
	CanSplit bool

	// key is the feature name now, not the *FeatMeta
	// This is specialized for categoricals, where you can
	// only be in one category, and that factor cannot be split again.
	Partitions map[string][]bool

	Loss float64
	Gain float64

	// middle/split node attributes
	Factor    FeatMeta
	Leftnode  *Node
	Rightnode *Node
	IsActive  bool
}

func (n *Node) String() (r string) {
	if n == nil {
		return "Node(ptr=nil)"
	}
	fstr := "(none;leaf)"
	if n.Factor.Name == "" {
		// means we are leaf, without splitting factor as of yet.
	} else {
		fstr = n.Factor.Name
	}
	opt := ""
	if false {
		// add in the partitions for small test cases
		opt = "\n\npartitions=\n" + fmt.Sprintf("%#v\n", n.Partitions)
	}

	return fmt.Sprintf("(%p)Node{IsLeaf:%v, Factor:%v, Leftnode:%p, Rightnode:%p, Gain:%v, Loss:%v, Gsum:%v Hsum:%v}", n, n.IsLeaf, fstr, n.Leftnode, n.Rightnode, n.Gain, n.Loss, n.Gsum, n.Hsum) + opt
}

func NewNode() (n *Node) {
	n = &Node{
		Partitions: make(map[string][]bool),
	}
	//vv("NewNode called, returning %p", n)
	return
}

type Tree struct {
	Nodes []*Node
}

type XGTreeEnsemble struct {
	Trees []*Tree
}

type XGModel struct {
	Ensemble XGTreeEnsemble
	Lambda   float64
	Gamma    float64
	Eta      float64
	Minh     float64
	Maxdepth int
	Pred     []float64
}

type TreeGrowState struct {
	NodeIDs []int
	Nodes   []*Node // only the leaf nodes, we get rid of Split nodes.

	Ensemble XGTreeEnsemble

	Factors *Matrix[int]

	// gradients
	Gcovariate []float64

	// hessians; only the diagonal though, presumably
	Hcovariate []float64

	Gamma       float64
	Lambda      float64
	MinH        float64
	SliceLength int

	Ordstumps    bool
	OptSplit     bool
	Pruning      bool
	Leafwise     bool
	Singlethread bool
}
