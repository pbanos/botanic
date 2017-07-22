package botanic

import (
	"context"
	"math"
	"sort"
)

/*
Partition represents a partition of a set according to a feature
into subtrees with an information gain to predict the class feature
*/
type Partition struct {
	feature         Feature
	subtrees        []*Tree
	informationGain float64
}

/*
NewDiscretePartition takes a set, a discrete feature and a class feature
and returns a partition of the set for the given feature. The result may be nil
if the obtained information gain is considered insufficient
*/
func NewDiscretePartition(ctx context.Context, s Set, f *DiscreteFeature, classFeature Feature, p Pruner) (*Partition, error) {
	availableValues := f.AvailableValues()
	subtrees := make([]*Tree, 0, len(availableValues))
	informationGain, err := s.Entropy(ctx, classFeature)
	if err != nil {
		return nil, err
	}
	count, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}
	totalCount := float64(count)
	for _, value := range availableValues {
		fc := NewDiscreteFeatureCriterion(f, value)
		subtree, err := NewTreeFromFeatureCriterion(ctx, fc, s)
		if err != nil {
			return nil, err
		}
		subtrees = append(subtrees, subtree)
		subtreeEntropy, err := subtree.set.Entropy(ctx, classFeature)
		if err != nil {
			return nil, err
		}
		subtreeCount, err := subtree.set.Count(ctx)
		if err != nil {
			return nil, err
		}
		informationGain -= subtreeEntropy * float64(subtreeCount) / totalCount
	}
	result := &Partition{f, subtrees, informationGain}
	ok, err := p.Prune(ctx, s, result, classFeature)
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	return result, nil
}

/*
NewContinuousPartition takes a set, a continuous feature and a class feature
and returns a partition of the set for the given feature. The result may be nil
if the obtained information gain is considered insufficient
*/
func NewContinuousPartition(ctx context.Context, s Set, f *ContinuousFeature, classFeature Feature, p Pruner) (*Partition, error) {
	sEntropy, err := s.Entropy(ctx, classFeature)
	if err != nil {
		return nil, err
	}
	result, err := newContinuousPartition(ctx, s, f, classFeature, sEntropy, math.Inf(-1), math.Inf(1), p)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	ok, err := p.Prune(ctx, s, result, classFeature)
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	return result, nil
}

/*
newRangePartition returns the partition of the given range in 2 parts that generates the most information gain
*/
func newRangePartition(ctx context.Context, s Set, f *ContinuousFeature, classFeature Feature, entropy, a, b float64) (*Partition, error) {
	var floatValues []float64
	sfvs, err := s.FeatureValues(ctx, f)
	if err != nil {
		return nil, err
	}
	for _, v := range sfvs {
		vf, _ := v.(float64)
		floatValues = append(floatValues, vf)
	}
	if len(floatValues) < 2 {
		return nil, nil
	}
	sort.Float64s(floatValues)
	var result *Partition
	for i, vf := range floatValues[1:] {
		threshold := (floatValues[i] + vf) / 2.0
		t1, err := NewTreeFromFeatureCriterion(ctx, NewContinuousFeatureCriterion(f, a, threshold), s)
		if err != nil {
			return nil, err
		}
		t2, err := NewTreeFromFeatureCriterion(ctx, NewContinuousFeatureCriterion(f, threshold, b), s)
		if err != nil {
			return nil, err
		}
		subtrees := []*Tree{t1, t2}
		informationGain := entropy
		count, err := s.Count(ctx)
		if err != nil {
			return nil, err
		}
		totalCount := float64(count)
		for _, subtree := range subtrees {
			subtreeEntropy, err := subtree.set.Entropy(ctx, classFeature)
			if err != nil {
				return nil, err
			}
			subtreeCount, err := subtree.set.Count(ctx)
			if err != nil {
				return nil, err
			}
			informationGain -= subtreeEntropy * float64(subtreeCount) / totalCount
		}
		if result == nil || result.informationGain < informationGain {
			result = &Partition{f, subtrees, informationGain}
		}
	}
	return result, nil
}

func newContinuousPartition(ctx context.Context, s Set, f *ContinuousFeature, classFeature Feature, entropy, a, b float64, p Pruner) (*Partition, error) {
	initialPartition, err := newRangePartition(ctx, s, f, classFeature, entropy, a, b)
	if err != nil {
		return nil, err
	}
	if initialPartition == nil {
		return nil, nil
	}
	ok, err := p.Prune(ctx, s, initialPartition, classFeature)
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	var resultSubtrees []*Tree
	informationGain := entropy
	count, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}
	totalCount := float64(count)
	for _, subtree := range initialPartition.subtrees {
		fc, _ := subtree.featureCriterion.(ContinuousFeatureCriterion)
		a, b := fc.Interval()
		subsetEntropy, err := subtree.set.Entropy(ctx, classFeature)
		if err != nil {
			return nil, err
		}
		subpartition, err := newContinuousPartition(ctx, subtree.set, f, classFeature, subsetEntropy, a, b, p)
		if err != nil {
			return nil, err
		}
		if subpartition == nil {
			subtreeCount, err := subtree.set.Count(ctx)
			if err != nil {
				return nil, err
			}
			resultSubtrees = append(resultSubtrees, subtree)
			informationGain -= subsetEntropy * float64(subtreeCount) / totalCount
		} else {
			for _, st := range subpartition.subtrees {
				stEntropy, err := st.set.Entropy(ctx, classFeature)
				if err != nil {
					return nil, err
				}
				stCount, err := st.set.Count(ctx)
				if err != nil {
					return nil, err
				}
				informationGain -= stEntropy * float64(stCount) / totalCount
				resultSubtrees = append(resultSubtrees, st)
			}
		}
	}
	return &Partition{f, resultSubtrees, informationGain}, nil
}
