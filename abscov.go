package gocortado

/*
import (
	"math"
)

func f_unique(acc map[float64]bool, slice []float64) {
	for _, v := range slice {
		_, found := acc[v]
		if !math.IsNaN(v) && np.isnan(v) && !acc[v] {
			acc[v] = v
		}
	}
	return acc
}

class AbstractCovariate(ABC):

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @property
    @abstractmethod
    def slicer(self):
        pass

    def to_array(self):
        slices = self.slicer(0, len(self), len(self))
        res, _ = Seq.try_read(slices)
        return res

    def cached(self):
        from cortado.cachedcovariate import CachedCovariate
        if isinstance(self, CachedCovariate):
            return self
        else:
            return CachedCovariate(self)

    def unique(self):
        slices = self.slicer(0, len(self), SLICELEN)
        # v, tail = Seq.try_read(slices)
        # set0 = set(v)

        # @jit(nopython=True, cache=True)
        # def f(acc, slice):
        #     for v in slice:
        #         if not np.isnan(v):
        #             acc.add(v)
        #     return acc
        # res = Seq.reduce(f, set0, tail)
        dt = types.float32 if self.slicer.dtype == np.float32 else types.float64
        set0 = Dict.empty(key_type=dt, value_type=dt)


        res = Seq.reduce(f_unique, set0, slices)

        arr = np.array(list(res.keys()), dtype=self.slicer.dtype)
        arr.sort()
        return arr

    def __repr__(self):
        k = min(HEADLENGTH, len(self))
        slices = self.slicer(0, k, HEADLENGTH)
        def f(acc, slice):
            return acc + ' '.join(["." if np.isnan(v) else str(v) for v in slice]) + " "
        datahead = Seq.reduce(f, "", slices)
        s = "" if k == len(self) else "..."
        return "Covariate {cov} with {len} obs: {head}{s}".format(cov= self.name, len= len(self), head= datahead, s = s)

    def __str__(self):
        return self.__repr__()
*/
