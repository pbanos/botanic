package botanic

import (
	"context"
	"math"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

// PruningStrategy holds the configuration
// for when a node not be partition further
// or at all.
type PruningStrategy struct {
	// Pruner is applied during the partition
	// of a node's dataset with a feature to determine
	// if the result is worth incorporating
	// into the tree.
	Pruner
	// MinimumEntropy is the maximum value of
	// entropy for a node that prevents it from
	// being branched out at all. In other words,
	// nodes whose training dataset has an
	// entropy equal or below this will not be
	// developed.
	MinimumEntropy float64
}

/*
Pruner is an interface wrapping the Prune method, that can be used
to decide whether a partition is good enough to become part of a tree
or if it must be pruned instead.

The Prune method takes a context, dataset, a partition and a class Feature and
returns a boolean: true to indicate the partition must be pruned, false to
allow its adding to the tree and further development.
*/
type Pruner interface {
	Prune(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error)
}

/*
PrunerFunc wraps a function with the Prune method signature to implement
the Pruner interface
*/
type PrunerFunc func(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error)

/*
Prune takes a context.Context, a dataset, a partition and a class Feature and
invokes the PrunerFunc with those parameters to return its boolean result.
*/
func (pf PrunerFunc) Prune(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error) {
	return pf(ctx, s, p, label)
}

/*
DefaultPruner returns a Pruner whose Prune method evaluates a minimum information
gain for the partition and returns true if the partition information gain is below
this minimum and false otherwise.
This minimum is calculated as
(1/N) x log2(N-1) + (1/N) x [ log2 (3k-2) - (k x Entropy(S) – k1 x Entropy(S1) – k2 x Entropy(S2) ... - ki x Entropy(Si)]
with
 * N begin the number of elements in the dataset
 * k being the number of different values for the label feature on the dataset
 * k1, k2, ... ki being the number of different values for the label feature on the subset for the partition subtree 1, 2, ... i
 * S1, S2, ... Si begin the subdataset for the partition subtree 1, 2, ... i
*/
func DefaultPruner() Pruner {
	return PrunerFunc(func(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error) {
		count, err := s.Count(ctx)
		if err != nil {
			return false, err
		}
		n := float64(count)
		fvs, err := s.FeatureValues(ctx, label)
		if err != nil {
			return false, err
		}
		k := float64(len(fvs))
		sEntropy, err := s.Entropy(ctx, label)
		if err != nil {
			return false, err
		}
		minimum := math.Log(n-1.0) + math.Log(math.Pow(3.0, k)-2) - k*sEntropy
		for _, st := range p.Tasks {
			stEntropy, err := st.Dataset.Entropy(ctx, label)
			if err != nil {
				return false, err
			}
			stfvs, err := st.Dataset.FeatureValues(ctx, label)
			if err != nil {
				return false, err
			}
			minimum += float64(len(stfvs)) * stEntropy
		}
		minimum = minimum / n
		return minimum > p.informationGain, nil
	})
}

/*
FixedInformationGainPruner takes an informationGainThreshold float64 value
and returns a Pruner whose Prune method returns whether the informationGainThreshold
is greater or equal to the received partition's information gain
*/
func FixedInformationGainPruner(informationGainThreshold float64) Pruner {
	return PrunerFunc(func(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error) {
		return informationGainThreshold >= p.informationGain, nil
	})
}

/*
NoPruner returns a Pruner whose Prune method always returns false, that is,
never prunes.
*/
func NoPruner() Pruner {
	return PrunerFunc(func(ctx context.Context, s dataset.Dataset, p *Partition, label feature.Feature) (bool, error) {
		return false, nil
	})
}
