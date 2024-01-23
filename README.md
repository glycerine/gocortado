# Go-Cortado: learn a gradient boosted decision trees classifier (ala Xgboost).

Status: Initial port complete. Needs generalization of the algorithm.

- The cortado project is Adam Mlocek's GBDT (Gradient Boosted Decision Tree) classifier in python
that utilizes lazy sequences and the numba JIT-compiler 
for numeric python code.

https://github.com/Statfactory/cortado

- I (Jason) did an initial attempt at porting cortado to Go.
That work is here. It compiles and runs, and now agrees
with the AUROC figure that cortado computes on the
airline training data set.

- Generalization beyond the initial training data set 
is missing from the original cortado code and from this Go port.

Theoretically, this should not be too hard to add. The 
current code would need the existing training data set available 
in order to do any prediction on out-of-sample data.

~~~
go test -v -run Test005_airline_data_test
~~~

Notes: 

- I did not attempt to port the Seq based lazy sequence
mechanisms used in cortado. I wrote Matrix, BoolMatrix,
and a simple iterator system instead. These allow quick flips
of a flag to change the iteration order, allowing further,
strategy changing, performance tuning. attic/seq.go has 
an old seq code attempt.

---

Copyright (C) 2023 by Jason E. Aten and Adam Mlocek.

Licence: MIT
