package gocortado

/*
// An uncompiled, mechanical translation of the f# version of split (Split.fs)
// code into Go, to try and grok it better in Go what is going on; without
// types, the python was pretty insrutible.
//
// Not guaranteed to run or even make sense at all; this is just
// a large language model's attempt at straight translation.

import (
	"math"
)


type Factor struct {
	Levels []int
}

type Covariate struct {
	SliceProvider interface{}
}

type DecisionNode interface{}

type LeafInfo struct {
	GSum       float32
	HSum       float32
	CanSplit   bool
	Partitions map[Factor][]bool
	Loss, Gain float64
}

type TreeLayer []DecisionNode

type XGTree []TreeLayer

type TreeGrowState struct {
	NodeIds     []int
	XGTree      XGTree
	Factors     []Factor
	GCovariate  Covariate
	HCovariate  Covariate
	Gamma       float32
	Lambda      float32
	MinH        float32
	SliceLength int
}


func getLoss(g float64, h float64, lambda float64) float64 {
	return -float64((g * g) / (h + lambda))
}

func mapSplitNode(issplitnode []bool, leftpartitions []bool, maxlevelcount int, factorindex []int, nodemap []int, nodeIdSlice []int, fslices []int) {

	slicelen := len(nodeIdSlice)
	nodeIdSliceSpan := nodeIdSlice
	fslicesSpan := fslices

	for i := 0; i < slicelen; i++ {
		nodeid := nodeIdSliceSpan[i]
		if issplitnode[nodeid] {
			levelindex := fslicesSpan[factorindex[nodeid]*slicelen+i]
			if leftpartitions[nodeid*maxlevelcount+levelindex] {
				nodemap[nodeIdSliceSpan[i]] = nodemap[nodeIdSliceSpan[i]]
			} else {
				nodemap[nodeIdSliceSpan[i]] = nodemap[nodeIdSliceSpan[i]] + 1
			}
		} else {
			nodemap[nodeIdSliceSpan[i]] = nodemap[nodeIdSliceSpan[i]]
		}
	}
}

func foldHist(acc0 []float64, acc1 []float64, acc2 []bool, acc3 int, zipslice []int, factorslice []int, gslice []float64, hslice []float64) ([]float64, []float64, []bool, int) {
	nodeslice := zipslice
	factorsliceSpan := factorslice
	gsliceSpan := gslice
	hsliceSpan := hslice

	for i := 0; i < len(nodeslice); i++ {
		nodeid := nodeslice[i]
		if acc2[nodeid] {
			levelindex := factorsliceSpan[i]
			acc0[nodeid*acc3+levelindex] += gsliceSpan[i]
			acc1[nodeid*acc3+levelindex] += hsliceSpan[i]
		}
	}
	return acc0, acc1, acc2, acc3
}

func getHistSlice(gsum0 []float64, hsum0 []float64, nodeids []int, nodecansplit []bool, factor Factor, gcovariate Covariate, hcovariate Covariate, start int, length int, slicelen int) (gsum []float64, hsum []float64) {

	nodeIdsSlicer := nodeids[start : start+length]
	levelCount := len(factor.Levels)
	nodeslices := nodeIdsSlicer

	// this needs help...
	// let factorslices = factor.SliceProvider.GetSlices(start, length, slicelen)
	// maybe this?
	factorslices := factorsliceProvider(factorslices, start, length, slicelen)

	gslices := gcovariate.SliceProvider.GetSlices(start, length, slicelen)
	hslices := hcovariate.SliceProvider.GetSlices(start, length, slicelen)

	zipslices := zip4(nodeslices, factorslices, gslices, hslices)
	var nodecansplit bool
	var levelCount int
	gsum, hsum, nodecansplit, levelCount = foldHist(gsum0, hsum0, nodecansplit, levelCount, zipslices)
	return gsum, hsum
}

func getHistogram(nodeIds []int, nodecansplit []bool, factor Factor, gcovariate Covariate, hcovariate Covariate, slicelen int) ([]float64, []float64) {
	nodecount := len(nodecansplit)
	levelcount := len(factor.Levels)
	start := 0
	length := len(nodeIds)

	gsum := make([]float64, nodecount*levelcount)
	hsum := make([]float64, nodecount*levelcount)

	gsum, hsum = getHistSlice(gsum, hsum, nodeIds, nodecansplit, factor, gcovariate, hcovariate, start, length, slicelen)
	return gsum, hsum
}

func getBestRangeSplit(g_hist []float64, h_hist []float64, partition []bool, lambda float64, minh float64) (gcurrloss, bestloss float64, leftPartition, rightPartition []bool, leftGSum, leftHSum, rightGSum, rightHSum float64) {

	var gsum, hsum float64

	for i := 0; i < len(g_hist); i++ {
		gsum += g_hist[i]
		hsum += h_hist[i]
	}
	gcurrloss := getLoss(gsum, hsum, lambda)
	bestloss := gcurrloss
	splitIndex := -1
	missLeft := true
	missG := g_hist[0]
	missH := h_hist[0]
	missActive := partition[0] && (missG+missH > 0) && (missH >= minh)
	leftGSum := float64(0)
	leftHSum := float64(0)
	rightGSum := gsum
	rightHSum := hsum

	gcumsum := float64(0)
	hcumsum := float64(0)

	if missActive {
		for i := 0; i < len(partition)-1; i++ {
			if partition[i] {
				gcumsum += g_hist[i]
				hcumsum += h_hist[i]
				lossMissLeft := getLoss(gcumsum, hcumsum, lambda) + getLoss(gsum-gcumsum, hsum-hcumsum, lambda)
				lossMissRight := lossMissLeft
				if i != 0 {
					lossMissRight = getLoss(gcumsum-missG, hcumsum-missH, lambda) + getLoss(gsum-gcumsum+missG, hsum-hcumsum+missH, lambda)
				}
				if lossMissLeft < bestloss && hcumsum >= minh && hsum-hcumsum >= minh {
					bestloss = lossMissLeft
					splitIndex = i
					missLeft = true
					leftGSum = gcumsum
					leftHSum = hcumsum
					rightGSum = gsum - gcumsum
					rightHSum = hsum - hcumsum
				}
				if lossMissRight < bestloss && hcumsum-missH >= minh && hsum-hcumsum+missH >= minh {
					bestloss = lossMissRight
					splitIndex = i
					missLeft = false
					leftGSum = gcumsum - missG
					leftHSum = hcumsum - missH
					rightGSum = gsum - gcumsum + missG
					rightHSum = hsum - hcumsum + missH
				}
			}
		}
	} else {
		for i := 0; i < len(partition)-1; i++ {
			if partition[i] {
				gcumsum += g_hist[i]
				hcumsum += h_hist[i]
				loss := getLoss(gcumsum, hcumsum, lambda) + getLoss(gsum-gcumsum, hsum-hcumsum, lambda)
				if loss < bestloss && hcumsum >= minh && hsum-hcumsum >= minh {
					bestloss = loss
					splitIndex = i
					leftGSum = gcumsum
					leftHSum = hcumsum
					rightGSum = gsum - gcumsum
					rightHSum = hsum - hcumsum
				}
			}
		}
	}

	if splitIndex >= 0 {
		leftPartition := make([]bool, len(partition))
		copy(leftPartition, partition)
		rightPartition := make([]bool, len(partition))
		copy(rightPartition, partition)

		for i := 0; i < len(partition); i++ {
			if partition[i] {
				if i == 0 && missActive {
					leftPartition[i] = missLeft
					rightPartition[i] = !missLeft
				} else {
					leftPartition[i] = i <= splitIndex
					rightPartition[i] = i > splitIndex
				}
			}
		}

		return gcurrloss, bestloss, leftPartition, rightPartition, leftGSum, leftHSum, rightGSum, rightHSum
	}

	return gcurrloss, bestloss, partition, partition, 0, 0, 0, 0
}

func getBestStumpSplit(g_hist []float64, h_hist []float64, partition []bool, lambda float64, minh float64) (float64, float64, []bool, float64, float64, float64, float64, float64) {
	gsum := float64(0)
	hsum := float64(0)
	for i := 0; i < len(g_hist); i++ {
		gsum += g_hist[i]
		hsum += h_hist[i]
	}
	gcurrloss := getLoss(gsum, hsum, lambda)
	bestloss := gcurrloss
	stumpIndex := -1
	minhFloat := float64(minh)

	for i := 0; i < len(partition); i++ {
		if partition[i] {
			loss := getLoss(g_hist[i], h_hist[i], lambda) + getLoss(gsum-g_hist[i], hsum-h_hist[i], lambda)
			if loss < bestloss && h_hist[i] >= minhFloat && hsum-h_hist[i] >= minhFloat {
				bestloss = loss
				stumpIndex = i
			}
		}
	}

	if stumpIndex >= 0 {
		leftPartition := make([]bool, len(partition))
		rightPartition := make([]bool, len(partition))
		copy(leftPartition, partition)
		copy(rightPartition, partition)

		for i := 0; i < len(partition); i++ {
			if partition[i] {
				if i == stumpIndex {
					rightPartition[i] = false
				} else {
					leftPartition[i] = false
				}
			}
		}

		return gcurrloss, bestloss, leftPartition, rightPartition, g_hist[stumpIndex], h_hist[stumpIndex], gsum - g_hist[stumpIndex], hsum - h_hist[stumpIndex]
	}

	return gcurrloss, bestloss, partition, partition, 0, 0, 0, 0
}

func getSplitNode(factor Factor, leafnode LeafInfo, histogram []float64, lambda float64, minh float64) DecisionNode {
	s := 0
	partitions := leafnode.Partitions[factor]
	for _, x := range partitions {
		if x {
			s++
		}
	}
	if !leafnode.CanSplit || s <= 1 {
		return leafnode
	} else {
		g_hist := histogram[:len(histogram)/2]
		h_hist := histogram[len(histogram)/2:]

		partition := partitions
		gsum := make([]float64, len(g_hist))
		hsum := make([]float64, len(h_hist))
		copy(gsum, g_hist)
		copy(hsum, h_hist)

		if factor.IsOrdinal {
			currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum :=
				getBestRangeSplit(gsum, hsum, partition, lambda, minh)
			if bestloss < currloss {
				leftpartitions := make(map[Factor][]bool)
				rightpartitions := make(map[Factor][]bool)
				for k, v := range leafnode.Partitions {
					leftpartitions[k] = make([]bool, len(v))
					rightpartitions[k] = make([]bool, len(v))
					copy(leftpartitions[k], v)
					copy(rightpartitions[k], v)
				}
				leftpartitions[factor] = leftpartition
				rightpartitions[factor] = rightpartition
				leftcansplit := false
				for _, v := range leftpartitions {
					sum := 0
					for _, x := range v {
						if x {
							sum++
						}
					}
					if sum > 1 {
						leftcansplit = true
						break
					}
				}
				rightcansplit := false
				for _, v := range rightpartitions {
					sum := 0
					for _, x := range v {
						if x {
							sum++
						}
					}
					if sum > 1 {
						rightcansplit = true
						break
					}
				}
				leftloss := getLoss(leftgsum, lefthsum, lambda)
				rightloss := getLoss(rightgsum, righthsum, lambda)
				leftnode := LeafInfo{GSum: leftgsum, HSum: lefthsum, CanSplit: leftcansplit, Partitions: leftpartitions, Loss: leftloss}
				rightnode := LeafInfo{GSum: rightgsum, HSum: righthsum, CanSplit: rightcansplit, Partitions: rightpartitions, Loss: rightloss}
				return Split{Factor: factor, LeftLeaf: leftnode, RightLeaf: rightnode, Loss: bestloss, Gain: currloss - bestloss}
			} else {
				return leafnode
			}
		} else {
			currloss, bestloss, leftpartition, rightpartition, leftgsum, lefthsum, rightgsum, righthsum :=
				getBestStumpSplit(gsum, hsum, partition, lambda, minh)
			if bestloss < currloss {
				leftpartitions := make(map[Factor][]bool)
				rightpartitions := make(map[Factor][]bool)
				for k, v := range leafnode.Partitions {
					leftpartitions[k] = make([]bool, len(v))
					rightpartitions[k] = make([]bool, len(v))
					copy(leftpartitions[k], v)
					copy(rightpartitions[k], v)
				}
				leftpartitions[factor] = leftpartition
				rightpartitions[factor] = rightpartition
				leftloss := getLoss(leftgsum, lefthsum, lambda)
				rightloss := getLoss(rightgsum, righthsum, lambda)
				leftnode := LeafInfo{GSum: leftgsum, HSum: lefthsum, CanSplit: false, Partitions: leftpartitions, Loss: leftloss}
				rightnode := LeafInfo{GSum: rightgsum, HSum: righthsum, CanSplit: false, Partitions: rightpartitions, Loss: rightloss}
				return Split{Factor: factor, LeftLeaf: leftnode, RightLeaf: rightnode, Loss: bestloss, Gain: currloss - bestloss}
			} else {
				return leafnode
			}
		}
	}
}

func getNewSplit(histograms [][2][]float64, nodes []LeafInfo, factor Factor, lambda float64, gamma float64, minh float64) []DecisionNode {
	levelCount := len(factor.Levels)
	g, h := histograms[0], histograms[1]
	newNodes := make([]DecisionNode, len(nodes))
	for i, node := range nodes {
		hist := [2][]float64{g[i], h[i]}
		newNodes[i] = getSplitNode(factor, node, hist, lambda, minh)
	}
	return newNodes
}

func findBestSplit(state TreeGrowState) []DecisionNode {
	layers := state.XGTree.Layers
	lastLayer := layers[len(layers)-1]
	nodeCanSplit := make([]bool, len(lastLayer))
	for i, node := range lastLayer {
		nodeCanSplit[i] = node.CanSplit
	}
	minGain := float64(state.Gamma)

	currSplit := make([]DecisionNode, len(lastLayer))
	copy(currSplit, lastLayer)

	for _, factor := range state.Factors {
		histograms := getHistogram(state.NodeIds, nodeCanSplit, factor, state.GCovariate, state.HCovariate, state.SliceLength)
		newSplit := getNewSplit(histograms, lastLayer, factor, state.Lambda, state.Gamma, state.MinH)
		for i, newNode := range newSplit {
			currNode := currSplit[i]
			if newNodeGain := newNode.(Split).Gain; newNodeGain > minGain && newNode.Loss < currNode.Loss {
				currSplit[i] = newNode
			}
		}
	}

	return currSplit
}

func updateState(state TreeGrowState, layerNodes []DecisionNode) TreeGrowState {

	newNodeIds := splitNodeIds(state.NodeIds, layerNodes, state.SliceLength)
	newLayer := []DecisionNode{}
	for _, node := range layerNodes {
		switch node := node.(type) {
		case Split:
			newLayer = append(newLayer, node.LeftLeaf, node.RightLeaf)
		case LeafInfo:
			newLayer = append(newLayer, node)
		}
	}
	newLayers := append(state.XGTree.Layers, newLayer)
	newXGTree := XGTree{Layers: newLayers}
	return TreeGrowState{
		NodeIds:     newNodeIds,
		XGTree:      newXGTree,
		Factors:     state.Factors,
		GCovariate:  state.GCovariate,
		HCovariate:  state.HCovariate,
		Gamma:       state.Gamma,
		Lambda:      state.Lambda,
		MinH:        state.MinH,
		SliceLength: state.SliceLength,
	}
}

func nextLayer(state TreeGrowState, step int) TreeGrowState {
	layerNodes := findBestSplit(state)
	return updateState(state, layerNodes)
}

func growTree(factors []Factor,
	gcovariate Covariate,
	hcovariate Covariate,
	fm []float64,
	eta float64,
	maxDepth int,
	lambda float64,
	gamma float64,
	minh float64,
	slicelen int,
) (XGTree, []float64) {

	nodeIds := make([]int, gcovariate.Length)
	nodes0 := []DecisionNode{{GSum: 0, HSum: 0, CanSplit: true,
		Partitions: map[Factor][]bool{}, Loss: math.MaxFloat64}}
	state := TreeGrowState{NodeIds: nodeIds, XGTree: XGTree{Layers: []Layer{nodes0}},
		Factors: factors, GCovariate: gcovariate, HCovariate: hcovariate, Gamma: gamma,
		Lambda: lambda, MinH: minh, SliceLength: slicelen}
	var fmCopy []float64
	copy(fmCopy, fm)

	for i := 0; i < maxDepth; i++ {
		state = nextLayer(state, i+1)
	}

	layers := state.XGTree.Layers
	lastLayer := layers[len(layers)-1]
	predict(lastLayer, state.NodeIds, fmCopy, eta, float64(lambda))
	return state.XGTree, fmCopy
}

func predict(nodes []DecisionNode, nodeIds []int, fm []float64, eta float64, lambda float64) {
	for i, node := range nodes {
		if nodeIds[i] == i {
			predictNode(node, fm, eta, lambda)
		}
	}
}

func predictNode(node DecisionNode, fm []float64, eta float64, lambda float64) {
	switch node := node.(type) {
	case Split:
		fm[node.FeatureIndex] *= float64(node.SplitCond)
		predictNode(node.LeftLeaf, fm, eta, lambda)
		predictNode(node.RightLeaf, fm, eta, lambda)
	case LeafInfo:
		g := float64(node.GSum)
		h := float64(node.HSum)
		score := g / (h + float64(lambda))
		fm[node.FeatureIndex] += eta * score
	}
}

func factorsliceProvider(factorslices []int, start int, length int, slicelen int) []int {
	result := make([]int, length)
	for i := 0; i < length; i++ {
		result[i] = factorslices[start+i]
	}
	return result
}

func splitNodeIds(nodeIds []int, layerNodes []DecisionNode, slicelen int) []int {
	result := make([]int, slicelen*len(layerNodes))
	for i, node := range layerNodes {
		for j := 0; j < slicelen; j++ {
			result[i*slicelen+j] = nodeIds[j]
		}
	}
	return result
}

func zip4(a []int, b []int, c []float64, d []float64) []int {
	result := make([]int, len(a)*4)
	for i := 0; i < len(a); i++ {
		result[i*4] = a[i]
		result[i*4+1] = b[i]
		result[i*4+2] = int(math.Float64bits(c[i]))
		result[i*4+3] = int(math.Float64bits(d[i]))
	}
	return result
}
*/
