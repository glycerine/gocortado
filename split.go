package gocortado

import (
	"fmt"
	"math"
)

//const skip_chunking = true // fastest in our quick and dirty benchmarks; 43.20s

const skip_chunking = false // 43.44s with slicelen = 10000
// skip_chunking = false ; slicelen = 1_000 =>   44.42s
// skip_chunking = false ; slicelen = 10_000 =>  43.44s
// skip_chunking = false ; slicelen = 100_000 => 43.41s
// skip_chunking = true                       => 43.20s

// fslices stands for factor slices
func f_splitnode(nodeslice []int, fslices *Matrix[int], issplitnode []bool, leftpartitions *BoolMatrix, factorindex []int, nodemap []int, maxlevelcount int) {

	if false {
		vv("top of f_splitnode")

		// jea: What is this doing? non-split nodes get nodeslice[i] = nodemap[nodeslice[i]],
		//   as do left partitions at this level... but otherwise
		//   nodemap[nodeslice[i]] +1 is assigned to nodeslice[i] ... ??? this does what?

		// go: f_splitnode: fslices is Nrow= 8  x Ncol= 10000
		vv("f_splitnode: nodeslice = '%#v'", nodeslice)
		vv("f_splitnode: fslices is %v", fslices) // fslices is too big, it should just be 'a' column but it has the whole factor matrix, on the a.20 test
		// python shows fslices; the 2nd arg:
		// f_splitnode: fslices is  [[13  3 10  7 12  4  5 19 18 20 17  9  8 16 14  1 15  2  6 11]]
		vv("f_splitnode: issplitnode = '%#v'", issplitnode)

		vv("f_splitnode: leftpartitions = '%#v'", leftpartitions)
		vv("f_splitnode: factorindex    = '%#v'", factorindex)
		vv("f_splitnode: nodemap        = '%#v'", nodemap)
	}
	// sanity check alignment
	slicelen := len(nodeslice)
	if fslices.Nrow != slicelen {
		panic(fmt.Sprintf("fslices.Nrow(%v) != slicelen(%v) == len(nodeslice)", fslices.Nrow, slicelen))
	}
	//vv("nodeslice = '%#v'", nodeslice)
	for i, nodeid := range nodeslice {
		if issplitnode[nodeid] {
			// works, gives 0.777 AUC
			levelindex := fslices.At(i, factorindex[nodeid])

			// AUC too low.
			// crashes now, i out of bounds. too low AUC.
			// but matches what the python looks like. So strange.
			// The Seq stuff might have been doing a transpose on us.
			//levelindex := fslices.At(factorindex[nodeid], i)

			//vv("on i=%v, nodeid=%v, issplitndoe[nodeid] is true, levelindex = %v", i, nodeid, levelindex)

			//if leftpartitions[nodeid*maxlevelcount+levelindex] {
			if leftpartitions.At(nodeid, levelindex) {
				//vv("leftpartitions.At(nodeid, levelindex) is true, assigning nodeslice[i=%v] = nodemap[nodeslice[i]] = %v", i, nodemap[nodeslice[i]])
				nodeslice[i] = nodemap[nodeslice[i]]
			} else {
				//vv("else leftpartitions.At(nodeid, levelindex) is false, assigning nodeslice[i=%v] = nodemap[nodeslice[i]]+1 = %v", i, nodemap[nodeslice[i]]+1)
				nodeslice[i] = nodemap[nodeslice[i]] + 1
			}
		} else {
			// INVAR: !issplitnode[nodeid]
			//vv("on i=%v, nodeid=%v, issplitndoe[nodeid] is false, assigning nodeslice[i] = nodemap[nodeslice[i]] = %v", i, nodeid, nodemap[nodeslice[i]])
			nodeslice[i] = nodemap[nodeslice[i]]
		}
	}
}

var global_f_hist_count int

func f_hist(gsum, hsum *Matrix[float64], nodecansplit []bool, nodeslice []int, factorslice []int, gslice, hslice []float64) {

	if false {
		global_f_hist_count++
		vv("global_f_hist_count = %v", global_f_hist_count)
		vv("top of f_hist, gslice(len %v) = head: '%#v'", len(gslice), gslice[:20])

		//vv("hslice is %v", hslice[:20])
		vv("nodeslice is %v", nodeslice[:20])
		vv("factorslice is %v", factorslice[:20])
	}

	//nodeslice, factorslice, gslice, hslice = zipslice()
	//gsum, hsum, nodecansplit = acc0
	for i, nodeid := range nodeslice {
		if nodecansplit[nodeid] {
			levelindex := factorslice[i]
			gsum.Add(int(nodeid), int(levelindex), gslice[i])
			hsum.Add(int(nodeid), int(levelindex), hslice[i])
		}
	}
}

func get_weight(g, h, lambda float64) float64 {
	return -g / (h + lambda)
}

func getloss(g, h, lambda float64) float64 {
	return -(g * g) / (h + lambda)
}

func sum(xs []float64) (tot float64) {
	for _, x := range xs {
		tot += x
	}
	return
}

func boolsum(xs []bool) (tot int) {
	for _, x := range xs {
		if x {
			tot++
		}
	}
	return
}

func get_best_stump_split(g_hist, h_hist []float64, partition []bool, lambda, minh float64) (currloss, bestloss float64, leftpartition, rightpartition []bool, leftgsum, lefthsum, rightgsum, righthsum float64) {

	gsum := SumSlice(g_hist)
	hsum := SumSlice(h_hist)
	currloss = getloss(gsum, hsum, lambda)
	bestloss = currloss
	stump_index := -1

	//for i := range partition {
	for i := 0; i < len(partition)-1; i++ {
		if !partition[i] {
			continue
		}
		loss := getloss(g_hist[i], h_hist[i], lambda) + getloss(gsum-g_hist[i], hsum-h_hist[i], lambda)
		if loss < bestloss && (h_hist[i] >= minh) && (hsum-h_hist[i] >= minh) {
			bestloss = loss
			stump_index = i
		}
	}
	if stump_index >= 0 {
		leftpartition = make([]bool, len(partition))
		rightpartition = make([]bool, len(partition))

		copy(leftpartition, partition)
		copy(rightpartition, partition)
		for i := range partition {
			if partition[i] {
				if i == stump_index {
					rightpartition[i] = false
				} else {
					leftpartition[i] = false
				}
			}
		}
		leftgsum = g_hist[stump_index]
		lefthsum = h_hist[stump_index]
		rightgsum = gsum - leftgsum
		righthsum = hsum - lefthsum
		return currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum
	} else {
		return currloss, bestloss, partition, partition, 0.0, 0.0, 0.0, 0.0
	}
}

func get_best_range_split(g_hist, h_hist []float64, partition []bool, lambda, minh float64) (currloss, bestloss float64, leftpartition, rightpartition []bool, leftgsum, lefthsum, rightgsum, righthsum float64) {

	gsum := SumSlice(g_hist)
	hsum := SumSlice(h_hist)
	currloss = getloss(gsum, hsum, lambda)
	bestloss = currloss
	split_index := -1
	miss_left := true
	miss_g := g_hist[0]
	miss_h := h_hist[0]
	miss_active := partition[0] && (miss_g+miss_h > 0.0) && miss_h >= minh

	gcumsum := 0.0
	hcumsum := 0.0
	if miss_active {
		for i := 0; i < len(partition)-1; i++ {
			if !partition[i] {
				continue
			}
			gcumsum += g_hist[i]
			hcumsum += h_hist[i]
			var loss_miss_right float64
			loss_miss_left := getloss(gcumsum, hcumsum, lambda) + getloss(gsum-gcumsum, hsum-hcumsum, lambda)
			if i == 0 {
				loss_miss_right = loss_miss_left
			} else {
				loss_miss_right = getloss(gcumsum-miss_g, hcumsum-miss_h, lambda) + getloss(gsum-gcumsum+miss_g, hsum-hcumsum+miss_h, lambda)
			}
			if loss_miss_left < bestloss && (hcumsum >= minh) && (hsum-hcumsum >= minh) {
				bestloss = loss_miss_left
				split_index = i
				miss_left = true
				leftgsum = gcumsum
				lefthsum = hcumsum
				rightgsum = gsum - gcumsum
				righthsum = hsum - hcumsum
			}

			if loss_miss_right < bestloss && (hcumsum-miss_h >= minh) && (hsum-hcumsum+miss_h >= minh) {
				bestloss = loss_miss_right
				split_index = i
				miss_left = false
				if i == 0 {
					leftgsum = gcumsum
					lefthsum = hcumsum
					rightgsum = gsum - gcumsum
					righthsum = hsum - hcumsum
				} else {
					leftgsum = gcumsum - miss_g
					lefthsum = hcumsum - miss_h
					rightgsum = gsum - gcumsum + miss_g
					righthsum = hsum - hcumsum + miss_h
				}
			}
		}
	} else {
		for i := 0; i < len(partition)-1; i++ {
			if !partition[i] {
				continue
			}
			gcumsum += g_hist[i]
			hcumsum += h_hist[i]
			loss := getloss(gcumsum, hcumsum, lambda) + getloss(gsum-gcumsum, hsum-hcumsum, lambda)
			if loss < bestloss && (hcumsum >= minh) && (hsum-hcumsum >= minh) {
				bestloss = loss
				split_index = i
				miss_left = true
				leftgsum = gcumsum
				lefthsum = hcumsum
				rightgsum = gsum - gcumsum
				righthsum = hsum - hcumsum
			}
		}
	}
	if split_index >= 0 {

		leftpartition = make([]bool, len(partition))
		rightpartition = make([]bool, len(partition))

		copy(leftpartition, partition)
		copy(rightpartition, partition)
		for i := range partition {
			if partition[i] {
				if i == 0 && miss_active {
					leftpartition[i] = miss_left
					rightpartition[i] = !miss_left
				} else {
					leftpartition[i] = i <= split_index
					rightpartition[i] = i > split_index
				}
			}
		}
		return currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum
	} else {
		return currloss, bestloss, partition, partition, 0.0, 0.0, 0.0, 0.0
	}
}

// same as hist stuff, make a non-chunked version too.
// the hist stuff it didn't matter, but maybe the nodeidslicing it will??
func split_nodeids_slice_skip_chunking(nodeids []int, factors []FeatMeta, issplitnode []bool, nodemap []int, leftpartitions *BoolMatrix, factorindex []int, start, length, slicelength int, featureMatrix *Matrix[int], maxlevelcount int) {

	if len(factors) == 0 {
		return
	}
	if false {
		vv("top of split_nodeids_slice_skip_chunking, factors (len %v):", len(factors))
		for i := range factors {
			vv("factors[%v].Name = '%v'", i, factors[i].Name)
		}
	}

	f_splitnode(nodeids, featureMatrix.ExtractFeatAsMatrix(factors), issplitnode, leftpartitions, factorindex, nodemap, maxlevelcount)
}

func split_nodeids_slice(nodeids []int, factors []FeatMeta, issplitnode []bool, nodemap []int, leftpartitions *BoolMatrix, factorindex []int, start, length, slicelength int, featureMatrix *Matrix[int], maxlevelcount int) {

	//vv("top of split_nodeids_slice, len(factors) = %v", len(factors))

	if len(factors) == 0 {
		return
	}

	//myFactor := factor.MyMat.(*Matrix[int]).Col(factor.Colj)

	//factorslices := NFactorSlicer(factors)(start, length, slicelength)
	factorslices := featureMatrix.NewRowColIter(factors, start, length, slicelength, "factorslices")

	nodeslices := NewVectorSlicer(nodeids, start, length, slicelength, "nodeslices")

	// 	SeqForEach(
	// 		func(nodeslice []int, fslices *Matrix[int]) {
	// 			f_splitnode(nodeslice, fslices, issplitnode, leftpartitions, factorindex, nodemap)
	// 		},
	// 		Seq.zip(nodeslices, factorslices))
	for {
		ns, done := nodeslices.FetchAdv()
		if done {
			break
		}
		f_splitnode(ns, factorslices.FetchAdv1(), issplitnode, leftpartitions,
			factorindex, nodemap, maxlevelcount)
	}
}

func split_nodeids(nodeids []int, nodes []*Node, slicelength int, featureMatrix *Matrix[int]) []int {
	nodecount := len(nodes)
	length := len(nodeids)
	start := 0
	issplitnode := make([]bool, len(nodes))
	//vv("split_nodeids issplitnode is len(nodes) = %v", len(nodes))
	for i, node := range nodes {
		issplitnode[i] = !node.IsLeaf
	}
	//vv("issplitnode	 = '%#v'", issplitnode)

	//issplitnode = [isinstance(n, SplitNode) for n in nodes]
	//issplitnode := make([]bool, len(nodes))
	//for i, n := range nodes {
	//	issplitnode[i] = !n.IsLeaf
	//}
	var nodemap []int
	splitnodecount := 0
	for i, x := range issplitnode {
		nodemap = append(nodemap, i+splitnodecount)
		if x {
			splitnodecount += 1
		}
	}
	//vv("splitnodecount = %v", splitnodecount)
	//nodemap = np.array(nodemap)
	//issplitnode = np.array(issplitnode)

	factormap := make(map[int]int)
	factorindex := make([]int, nodecount)
	var factors []FeatMeta
	//factorindex := make([]int, nodecount)
	//leftpartitions := make(map[*FeatMeta][]bool)
	var leftpartitions *BoolMatrix
	var maxlevelcount int
	for i := 0; i < nodecount; i++ {
		if issplitnode[i] {
			//vv("issplitnode[i=%v] is true", i)
			factor := nodes[i].Factor

			pos, already := factormap[factor.Colj]
			if !already {
				pos = len(factors)
				factormap[factor.Colj] = pos
				//vv("setting factormap[factor.Colj=%v] = pos = %v", factor.Colj, pos)
				//vv("appending factor '%v' to factors", factor.Name)
				factors = append(factors, factor)
			}
			factorindex[i] = pos
		} else {
			factorindex[i] = -1
		}

		for i, n := range nodes {
			if !n.IsLeaf {
				// compute max over n.Leftnode.Partitions[n.factor]
				tmp := len(n.Leftnode.Partitions[n.Factor.Name])
				if i == 0 || tmp > maxlevelcount {
					maxlevelcount = tmp
				}
			}
		}
		//maxlevelcount = max([len(n.Leftnode.Partitions[n.factor]) if isinstance(n, SplitNode) else 0 for n in nodes])
		leftpartitions = NewBoolMatrix(nodecount, maxlevelcount)
		for i := 0; i < nodecount; i++ {
			if issplitnode[i] {
				n := nodes[i]
				leftpart := n.Leftnode.Partitions[n.Factor.Name]

				for j := range leftpart {
					leftpartitions.Set(i, j, leftpart[j])
				}
				//leftpartitions[i, :len(leftpart)] = leftpart
			}
		}
	}
	// factors is too big coming in here... should just be 'a' but we have the whole thing.
	if skip_chunking {
		split_nodeids_slice_skip_chunking(nodeids, factors, issplitnode, nodemap, leftpartitions, factorindex,
			start, length, slicelength, featureMatrix, maxlevelcount)
	} else {
		split_nodeids_slice(nodeids, factors, issplitnode, nodemap, leftpartitions, factorindex,
			start, length, slicelength, featureMatrix, maxlevelcount)
	}
	return nodeids
}

func get_splitnode(factor FeatMeta, leafnode *Node, g_hist, h_hist []float64, lambda, minh float64) *Node {

	partition := leafnode.Partitions[factor.Name]

	var currloss, bestloss float64
	var leftpartition, rightpartition []bool
	var leftgsum, lefthsum, rightgsum, righthsum float64

	if factor.IsOrdinal {
		currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum = get_best_range_split(g_hist, h_hist, partition, lambda, minh)
	} else {
		currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum = get_best_stump_split(g_hist, h_hist, partition, lambda, minh)
	}

	if bestloss < currloss {

		//python:
		//leftpartitions = {k : v for k, v in leafnode.Partitions.items()}
		//rightpartitions = {k : v for k, v in leafnode.Partitions.items()}

		//Go:
		leftpartitions := make(map[string][]bool)
		rightpartitions := make(map[string][]bool)
		for k, v := range leafnode.Partitions {
			leftpartitions[k] = v
			rightpartitions[k] = v
		}

		leftpartitions[factor.Name] = leftpartition
		rightpartitions[factor.Name] = rightpartition

		// jea: why is these not >= 1 ? i.e. why are 2 leftpartition values required to split?
		// instead of just 1?
		//  must have 2 levels left in a factor in order to split,
		//  that is why >1  levels left is required. but the missing data level... how
		// is that handled/excluded.

		//leftcansplit = any([sum(v) > 1 for k, v in leftpartitions.items()])
		var leftcansplit bool
		for _, v := range leftpartitions {
			if boolsum(v) > 1 {
				leftcansplit = true
				break
			}
		}

		//rightcansplit = any([sum(v) > 1 for k, v in rightpartitions.items()])
		var rightcansplit bool
		for _, v := range rightpartitions {
			if boolsum(v) > 1 {
				rightcansplit = true
				break
			}
		}

		leftloss := getloss(leftgsum, lefthsum, lambda)
		rightloss := getloss(rightgsum, righthsum, lambda)

		leftnode := &Node{IsLeaf: true, Gsum: leftgsum, Hsum: lefthsum, CanSplit: leftcansplit, Partitions: leftpartitions, Loss: leftloss}
		rightnode := &Node{IsLeaf: true, Gsum: rightgsum, Hsum: righthsum, CanSplit: rightcansplit, Partitions: rightpartitions, Loss: rightloss}
		return &Node{IsLeaf: false, Factor: factor, Leftnode: leftnode, Rightnode: rightnode, Loss: bestloss, Gain: currloss - bestloss}
	} else {
		return nil
	}
}

// avoid allocation pressure by reusing these;
// only useful for single threaded though.

func getnewsplit(gsum, hsum *Matrix[float64], nodes []*Node, factor FeatMeta, lambda, gamma, minh float64) []*Node {
	newsplit := make([]*Node, len(nodes))

	for i := range nodes {
		if nodes[i].IsLeaf && nodes[i].CanSplit && boolsum(nodes[i].Partitions[factor.Name]) > 1 {
			newsplit[i] = get_splitnode(factor, nodes[i], gsum.Row(i), hsum.Row(i), lambda, minh)
		}
	}

	if false {
		vv("newsplit determined: ")
		for i := range nodes {
			str := "nil Node"
			if newsplit[i] != nil {
				str = fmt.Sprintf("%#v", newsplit[i])
			}
			fmt.Printf("   newsplit[i=%v] = '%v'; given gsum.Row(i)='%v' and hsum.Row(i)='%v'\n", i, str, gsum.Row(i), hsum.Row(i))
		}
	}
	return newsplit
}

func update_state(state *TreeGrowState, layernodes []*Node) *TreeGrowState {

	//vv("top of update_state")

	split_nodeids(state.NodeIDs, layernodes, state.SliceLength, state.Factors)

	var newnodes []*Node
	// so layernodes has only the interior leaves?
	// maybe: we append the children of the layernodes ... but
	// we also check to see if it is a leaf or not... so SOME nodes
	// in layernodes might be leaves already, but maybe the
	// list is not complete?? could we end up with duplicates???
	for _, n := range layernodes {
		if !n.IsLeaf {
			newnodes = append(newnodes, n.Leftnode)
			newnodes = append(newnodes, n.Rightnode)
		} else {
			newnodes = append(newnodes, n)
		}
	}
	state.Nodes = newnodes
	return state
}

func nextlayer(state *TreeGrowState) ([]*Node, *TreeGrowState) {
	layernodes := find_best_split(state)
	update_state(state, layernodes)
	return layernodes, state
}

// fm is updated in place by f_weights and so by us; but we'll
// return it just in case this changes/a reallocation of the
// backing array is forced.
func predict(treenodes []*Node, nodeids []int, fm []float64, eta, lambda float64) []float64 {

	if false {
		vv("predict called on treenodes = '%#v'", treenodes)
		for i := range treenodes {
			vv("treenodes[%v] = '%v'", i, treenodes[i])
		}
		// we have the leaves but not the split node!
		//split.go:521 2023-08-20T14:36:26.403-05:00 treenodes[0] = '(0xc000079a00)Node{IsLeaf:true, Factor:(none;leaf), Leftnode:0x0, Rightnode:0x0, Gain:0, Loss:-7.142857142857143, Gsum:5 Hsum:2.5}'

		//split.go:521 2023-08-20T14:36:26.431-05:00 treenodes[1] = '(0xc000079ad0)Node{IsLeaf:true, Factor:(none;leaf), Leftnode:0x0, Rightnode:0x0, Gain:0, Loss:-7.142857142857143, Gsum:-5 Hsum:2.5}'

		vv("predict call with nodeids='%#v'", nodeids) // split.go:528 2023-08-20T14:39:44.173-05:00 predict call with nodeids='[]int{1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1}'
		// for the a.20 test case
		expected := []int{1, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1}
		for i, x := range expected {
			if nodeids[i] != x {
				panic(fmt.Sprintf("expected nodeids[%v] = '%v' but got '%v'", i, x, nodeids[i]))
			}
		}
		vv("predict call with fm='%#v'", fm)
		vv("predict call with eta='%v'", eta)
		vv("predict call with lambda='%v'", lambda)
	}

	if lambda == 0 {
		panic("lambda better not be 0 in predict()")
	}
	weights := make([]float64, len(treenodes))
	//weights = np.empty(len(treenodes), dtype=np.float32)
	//for (i, node) in enumerate(treenodes) {
	for i, node := range treenodes {
		weights[i] = -node.Gsum / (node.Hsum + lambda)
	}
	return f_weights(nodeids, weights, fm, eta)
}

func f_weights(nodeids []int, weights []float64, fm []float64, eta float64) []float64 {
	//vv("f_weights called with nodeids='%#v' weights='%#v', fm='%#v', eta='%v'", nodeids, weights, fm, eta)
	for i, nii := range nodeids {
		fm[i] = eta*weights[nii] + fm[i]
	}
	return fm
}

func makeAllTrueBoolSlice(n int) []bool {
	r := make([]bool, n)
	for i := range r {
		r[i] = true
	}
	return r
}

// nodecansplit is not changed inside here, so no need to return it.
// hsum and gsum are updated, but they are not slices so no need to return them either.
//
// for gradients/hessians: python uses float32, but Go will use float64, at least to start;
// at least until we can check rigorously for numerical stability, which is typically
// awful with float32.
//
// we are seeing the biggest differences with cortado at the 1st chunk boundary,
// so try a version with no chunking and rename this original.
//
// This is the chunked version.
func get_hist_slice(gsum, hsum *Matrix[float64],

	nodeids []int,
	nodecansplit []bool,
	factor FeatMeta,
	gcovariate, hcovariate []float64,
	start,
	length,
	slicelen int, // typically 10000

) {
	//vv("get_hist_slice: nodeids: len='%v'; myFactor is %p named '%v'", len(nodeids), factor, factor.Name) // DepTime needs MyMat adjusted from float64 to the int Matrix
	//vv("myFactor is %p named '%v': '%v'", factor, factor.Name, factor) // DepTime needs MyMat adjusted from float64 to the int Matrix
	myFactor := factor.MyMat.(*Matrix[int]).Col(factor.Colj)

	nodeslices2 := NewVectorSlicer(nodeids, start, length, slicelen, "nodeslices2")
	factorslices := NewVectorSlicer(myFactor, start, length, slicelen, "factorslices")
	gslices := NewVectorSlicer(gcovariate, start, length, slicelen, "gslices")
	hslices := NewVectorSlicer(hcovariate, start, length, slicelen, "hslices")

	//vv("gcovariate (len %v) = '%#v'", len(gcovariate), gcovariate[:20])

	//vv("hcovariate (len %v) = '%#v'", len(hcovariate), hcovariate[:20])

	//zipslices := Seq.zip(nodeslices, factorslices, gslices, hslices)
	//return Seq.reduce(f_hist, (gsum0, hsum0, nodecansplit), zipslices)

	for {
		ns, done := nodeslices2.FetchAdv()
		if done {
			break
		}
		// with slicelen = 10000
		// gslice is  (10000,) ## float32, <class 'numpy.dtype[float32]'>
		// hslice is  (10000,) ## float32, <class 'numpy.dtype[float32]'>
		// nodeslice is  (10000,) shape.  <class 'numpy.dtype[uint16]'>
		// factorslice is  (10000,) shape. <class 'numpy.dtype[uint8]'>

		// func f_hist(gsum0, hsum0 *Matrix[float64], nodecansplit0 []bool, nodeslice []uint16, factorslice []uint8, gslice, hslice []float32)
		f_hist(gsum, hsum, nodecansplit, ns, factorslices.FetchAdv1(), gslices.FetchAdv1(), hslices.FetchAdv1())
	}
	return
}

func get_hist_slice_skip_chunking(gsum, hsum *Matrix[float64],

	nodeids []int,
	nodecansplit []bool,
	factor FeatMeta,
	gcovariate, hcovariate []float64,
	start,
	length,
	slicelen int, // typically 10000

) {
	myFactor := factor.MyMat.(*Matrix[int]).Col(factor.Colj)
	f_hist(gsum, hsum, nodecansplit, nodeids, myFactor, gcovariate, hcovariate)
	return
}

func get_histogram(nodeids []int, nodecansplit []bool, factor FeatMeta, gcovariate, hcovariate []float64, slicelen int) (gsum, hsum *Matrix[float64]) {

	nodecount := len(nodecansplit)

	levelcount := len(factor.Levels)
	start := 0
	length := len(nodeids)

	//vv("levelcount = %v", levelcount)
	//vv("factor = '%v'", factor.Name)
	//vv("nodeids len =%v; ", length) // there are 1e6, do not print them! and well, they are all 0 for go...

	//gsum = NewMatrix[float64](nodecount, levelcount)
	//hsum = NewMatrix[float64](nodecount, levelcount)
	//gsum = NewMatrixColVec[float64](nodecount, levelcount)
	//hsum = NewMatrixColVec[float64](nodecount, levelcount)
	gsum = NewMatrixColMajor[float64](nodecount, levelcount)
	hsum = NewMatrixColMajor[float64](nodecount, levelcount)

	if skip_chunking {
		get_hist_slice_skip_chunking(gsum, hsum, nodeids, nodecansplit, factor, gcovariate, hcovariate, start, length, slicelen)
	} else {
		get_hist_slice(gsum, hsum, nodeids, nodecansplit, factor, gcovariate, hcovariate, start, length, slicelen)
	}
	return gsum, hsum
}

func find_best_split(state *TreeGrowState) (besties []*Node) {

	if false {
		defer func() {
			for i := range besties {
				vv("besties[%v of %v] = '%v'", i, len(besties), besties[i])
				vv("besties[%v].leftnode = '%v'", i, besties[i].Leftnode)
				vv("besties[%v].rightnode = '%v'", i, besties[i].Rightnode)
			}
		}()
	}

	nodecansplit := make([]bool, len(state.Nodes))
	for i, n := range state.Nodes {
		if n.IsLeaf && n.CanSplit {
			nodecansplit[i] = true
		}
	}
	//nodecansplit = [isinstance(n, LeafNode) && n.CanSplit for n in state.Nodes]
	mingain := state.Gamma

	f := func(currsplit []*Node, factor FeatMeta) []*Node {

		gsum, hsum := get_histogram(state.NodeIDs, nodecansplit, factor, state.Gcovariate, state.Hcovariate, state.SliceLength)

		//vv("in find_best_split -> f: gsum[10000:10002] = '%v'", gsum.Row(0)[10000:10002])
		//vv("in find_best_split -> f: hsum[10000:10002] = '%v'", hsum.Row(0)[10000:10002])
		newsplit := getnewsplit(gsum, hsum, state.Nodes, factor, state.Lambda, state.Gamma, state.MinH)

		//vv("newsplit = '%v'", newsplit)
		res := make([]*Node, len(newsplit))
		//res = [None for i in range(len(newsplit))]
		for i := 0; i < len(newsplit); i++ {
			if newsplit[i] != nil {
				newloss := newsplit[i].Loss
				newgain := newsplit[i].Gain
				if newgain > mingain && newloss < currsplit[i].Loss {
					res[i] = newsplit[i]
				} else {
					res[i] = currsplit[i]
				}
			} else {
				res[i] = currsplit[i]
			}
		}
		return res
	}
	//return reduce(f, state.factors, state.Nodes)
	nodes := state.Nodes
	for _, feat := range state.Factors.Cmeta {
		nodes = f(nodes, feat)
	}
	return nodes
}

func check_nodes(nodes []*Node, note string) {
	u := make(map[*Node]bool)
	for _, node := range nodes {
		if node == nil {
			panic("Where? nil node")
		}
		_, already := u[node]
		if already {
			panic(fmt.Sprintf("found duplicate node in '%v': '%v'", note, node))
		} else {
			u[node] = true
		}
	}
}

// create one new tree to add to the ensemble
func growtree(factors *Matrix[int], gcovariate, hcovariate, fm []float64, eta float64, maxdepth int, lambda, gamma, minh float64, slicelen int, singlethreaded bool, leafwise bool) (curTree *Tree, predraw []float64) {

	length := len(gcovariate)

	// maxnodecount was only used in split.py to make
	// sure nodeids was using uint8 or uint16, enough to capture
	//  any location in the tree.
	//maxNodeCount := 1 << maxdepth

	// I suspect nodeids might be the node that each case ends up in, and
	// thus forms the "prediction" of the target category for that case.
	// because there were checks above to make sure that the integer
	// used is enough to express all possible nodes. We are just
	// using int now for simplicity and to support fairly arbitrary depth.
	nodeids := make([]int, length)

	if length != factors.Nrow {
		panic(fmt.Sprintf("how can len of gcovariate(%v) != factors.Nrow = %v ??", length, factors.Nrow))
	}
	//vv("length = %v", length)

	//nodeids = np.zeros(length, dtype=np.uint8) if maxnodecount <= np.iinfo(np.uint8).max else np.zeros(length, dtype=np.uint16)

	// partitions are type map[*FeatMeta]*LevelPartition
	// where
	// type LevelPartition struct {
	//    mask        []bool
	//    inclmissing bool
	// }

	loss0 := math.MaxFloat32
	//part := make(map[*FeatMeta][]bool)

	n0 := NewNode()
	n0.IsLeaf = true
	n0.CanSplit = true
	n0.Loss = loss0
	for _, f := range factors.Cmeta {
		n0.Partitions[f.Name] = NewLevelPartitionAllTrue(len(f.Levels))
	}

	nodes0 := []*Node{n0}
	//nodes0 = [LeafNode((0.0, 0.0), true, {f : np.full(len(f.levels), true) for f in factors}, loss0)]

	state := &TreeGrowState{
		NodeIDs:     nodeids,
		Nodes:       nodes0,
		Factors:     factors,
		Gcovariate:  gcovariate,
		Hcovariate:  hcovariate,
		Gamma:       gamma,
		Lambda:      lambda,
		MinH:        minh,
		SliceLength: slicelen,
	}
	_ = state

	var layernodes []*Node
	for i := 0; i < maxdepth; i++ {
		// nextlayer does:
		layernodes = find_best_split(state)
		update_state(state, layernodes)
		check_nodes(layernodes, "after update_state")
	}

	// fm is updated in place by predict()
	fm = predict(state.Nodes, nodeids, fm, eta, lambda)
	return &Tree{Nodes: layernodes}, fm
}
