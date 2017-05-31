package botanic

import "math"

/*
Pruner is an interface wrappint the Prune method, that can be used
to decide whether a partition is good enough to become part of a tree
or if it must be pruned instead.

The Prune method takes a set, a partition and a class Feature and returns
boolean: true to indicate the partition must be pruned, false to allow its
adding to the tree and further development.
*/
type Pruner interface {
	Prune(s Set, p *Partition, classFeature Feature) bool
}

/*
PrunerFunc wraps a function with the Prune method signature to implement
the Pruner interface
*/
type PrunerFunc func(s Set, p *Partition, classFeature Feature) bool

/*
Prune takes a set, a partition and a class Feature and invokes the
PrunerFunc with those parameters to return its boolean result.
*/
func (pf PrunerFunc) Prune(s Set, p *Partition, classFeature Feature) bool {
	return pf(s, p, classFeature)
}

/*
DefaultPruner returns a Pruner whose Prune method evaluates a minimum information
gain for the partition and returns true if the partition information gain is below
this minimum and false otherwise.
This minimum is calculated as
(1/N) x log2(N-1) + (1/N) x [ log2 (3k-2) - (k x Entropy(S) – k1 x Entropy(S1) – k2 x Entropy(S2) ... - ki x Entropy(Si)]
with
 * N begin the number of elements in the set
 * k being the number of different values for the class feature on the set
 * k1, k2, ... ki being the number of different values for the class feature on the subset for the partition subtree 1, 2, ... i
 * S1, S2, ... Si begin the subset of data for the partition subtree 1, 2, ... i
*/
func DefaultPruner() Pruner {
	return PrunerFunc(func(s Set, p *Partition, classFeature Feature) bool {
		n := float64(s.Count())
		k := float64(len(s.FeatureValues(classFeature)))
		minimum := math.Log(n-1.0) + math.Log(math.Pow(3.0, k)-2) - k*s.Entropy(classFeature)
		for _, st := range p.subtrees {
			minimum += float64(len(st.set.FeatureValues(classFeature))) * st.set.Entropy(classFeature)
		}
		minimum = minimum / n
		return minimum > p.informationGain
	})
}

/*
FixedInformationGainPruner takes an informationGainThreshold float64 value
and returns a Pruner whose Prune method returns whether the informationGainThreshold
is greater or equal to the received partition's information gain
*/
func FixedInformationGainPruner(informationGainThreshold float64) Pruner {
	return PrunerFunc(func(s Set, p *Partition, classFeature Feature) bool {
		return informationGainThreshold >= p.informationGain
	})
}

/*
NoPruner returns a Pruner whose Prune method always returns false, that is,
never prunes.
*/
func NoPruner() Pruner {
	return PrunerFunc(func(s Set, p *Partition, classFeature Feature) bool {
		return false
	})
}
