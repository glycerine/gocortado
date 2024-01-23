package gocortado

import (
	"fmt"
	"math"
	"os"
	"time"
)

func logit_g(yhat, y float64) float64 {
	return yhat - y
}

const eps = 1.1920929e-07 // np.finfo(np.float32).eps

func logit_h(yhat float64) float64 {
	//eps = np.finfo(np.float32).eps
	tmp := yhat * (1 - yhat)
	if tmp < eps {
		return eps
	}
	return tmp
}

func logitraw(p float64) float64 {
	return -math.Log(1/p - 1)
}

func sigmoid(x float64) float64 {
	return 1 / (1 + math.Exp(-x))
}

func get_gh_sel(selector []bool, yhatraw, label, g, h []float64) {
	for i := range g {
		if selector[i] {
			yhat := sigmoid(yhatraw[i])
			y := label[i]
			g[i] = logit_g(yhat, y)
			h[i] = logit_h(yhat)
		}
	}
}
func get_gh(yhatraw, label, g, h []float64) {
	for i := range g {
		yhat := sigmoid(yhatraw[i])
		y := label[i]
		g[i] = logit_g(yhat, y)
		h[i] = logit_h(yhat)
		//if i < 5 {
		//	println("i = ", i, " yhat = ", yhat, " y = ", y, " g[i] = ", g[i], " h[i] = ", h[i])
		//}
	}
}

func get_pred(fm []float64) {
	for i, v := range fm {
		fm[i] = sigmoid(v)
	}
}

// label is the target, typically 0,1 for logistic regression problems.
// mu is probably the mean reponse (label/Y mean or target mean)
// trainselector[i] is true if case i is used for training.
// eta is the learning rate, typically 0.1
func xgblogit(label []float64, factors *Matrix[int], trainselector []bool, mu float64, eta float64, lambda_ float64, gamma float64, maxdepth int, nrounds int, minh float64, slicelen int) (model *XGModel, predictions []float64) {

	if mu == 0 {
		mu = 0.5
	}
	// f0 and fm are yhatraw/residuals after each round
	f0 := make([]float64, len(label))
	lr0 := logitraw(mu)
	for i := range f0 {
		f0[i] = lr0
	}

	// current gradient and hessian (hessian diagonal elem)
	g := make([]float64, len(f0))
	h := make([]float64, len(f0))

	addOneTree := func(fm []float64, trees0 []*Tree) (predraw []float64, trees []*Tree) {

		if len(trainselector) > 0 {
			get_gh_sel(trainselector, fm, label, g, h)
		} else {
			get_gh(fm, label, g, h)
			// go: after get_gh, g = '[]float64{0, 0, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0, -1, 0, 0, -1}'
			// python: after get_gh, g =  [ 0.5  0.5  0.5 ... -0.5  0.5  0.5]
			//vv("after get_gh, g = '%#v'", g[:20])
		}
		//g_cov := Covariate_from_array(g)
		//h_cov := Covariate_from_array(h)
		//tree, predraw := growtree(factors, g_cov, h_cov, fm, eta, maxdepth, lambda_, gamma, minh, slicelen)

		const singlethreaded = true
		leafwise := true
		tree, predraw := growtree(factors, g, h, fm, eta, maxdepth, lambda_, gamma, minh, slicelen, singlethreaded, leafwise)
		trees = append(trees0, tree)
		//vv("back from growtree() round %v", i)
		//vv("back from growtree(); sum predraw = %v", sum(predraw))
		//vv("back from growtree() predraw = %#v", predraw)

		return predraw, trees
	}
	//fm, trees = Seq.reduce(step, (f0, []), Seq.from_gen((i for i in range(nrounds))))

	//vv("f0 = '%#v'", f0)
	t0 := time.Now()

	fm := f0
	var trees []*Tree
	for i := 0; i < nrounds; i++ {
		t0tree1 := time.Now()

		fm, trees = addOneTree(fm, trees)
		// TODO: check for the early stopping condition based on a validation set.
		//vv("after i=%v round, len(trees) = '%v'", i, len(trees))

		if false {
			vv("time to train that last tree = '%v' ; on round i=%v of nrounds=%v, maxdepth=%v", time.Since(t0tree1), i, nrounds, maxdepth)

			// check auc of preds - but this is very slow

			pred1 := make([]float64, len(fm))
			copy(pred1, fm) // make a copy we can apply the sigmoid to and not change fm
			//vv("round i = %v, fm before get_pred sum = %v, (len %v; first 20 here) = '%#v'", i, sum(pred1), len(pred1), pred1[:20])
			get_pred(pred1)
			//vv("round i = %v, fm after inline get_pred sum = %v, (len %v; first 20 here) = '%#v'", i, sum(pred1), len(pred1), pred1[:20])

			pred1count := 0
			targ1count := 0
			for k, pr := range pred1 {
				if pr > 0.5 {
					pred1count++
				}
				if label[k] > 0.5 {
					targ1count++
				}
			}
			vv("pred1count = %v ; targ1count = %v", pred1count, targ1count)

			// printing predictions out is slow...
			fd, err := os.Create("pred_y_vft.csv")
			panicOn(err)
			fmt.Fprintf(fd, "pred,y\n")
			for i := range pred1 {
				fmt.Fprintf(fd, "%v,%v\n", pred1[i], label[i])
			}
			fd.Close()

			//y := target
			//auc := roc_auc_score(y, pred)
			aucroc, err := AreaUnderROC(pred1, label, []*TargetRange{&TargetRange{Thresh: 0.5}})
			panicOn(err)
			vv("on round i = %v, gocortado auc = %v", i, aucroc[0].Auc1)
		}

	}

	vv("total time to train all %v rounds: %v ; maxdepth = %v", nrounds, time.Since(t0), maxdepth)

	vv("before get_pred, sum fm = %v, fm[:20 = %v", sum(fm), fm[:20])

	// apply a sigmoid to fm
	get_pred(fm)

	vv("after get_pred, sum fm = %v, fm[:20 = %v", sum(fm), fm[:20])

	return &XGModel{
		Ensemble: XGTreeEnsemble{Trees: trees},
		Lambda:   lambda_,
		Gamma:    gamma,
		Eta:      eta,
		Minh:     minh,
		Maxdepth: maxdepth,
		Pred:     fm,
	}, fm

}
