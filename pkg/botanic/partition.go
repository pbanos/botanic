package botanic

import (
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
func NewDiscretePartition(s Set, f *DiscreteFeature, classFeature Feature, p Pruner) *Partition {
	availableValues := f.AvailableValues()
	subtrees := make([]*Tree, 0, len(availableValues))
	informationGain := s.Entropy(classFeature)
	totalCount := float64(s.Count())
	for _, value := range availableValues {
		fc := NewDiscreteFeatureCriterion(f, value)
		subtree := NewTreeFromFeatureCriterion(fc, s)
		subtrees = append(subtrees, subtree)
		informationGain -= subtree.set.Entropy(classFeature) * float64(subtree.set.Count()) / totalCount
	}
	result := &Partition{f, subtrees, informationGain}
	if p.Prune(s, result, classFeature) {
		return nil
	}
	return result
}

/*
NewContinuousPartition takes a set, a continuous feature and a class feature
and returns a partition of the set for the given feature. The result may be nil
if the obtained information gain is considered insufficient
*/
func NewContinuousPartition(s Set, f *ContinuousFeature, classFeature Feature, p Pruner) *Partition {
	result := newContinuousPartition(s, f, classFeature, s.Entropy(classFeature), math.Inf(-1), math.Inf(1), p)
	if result == nil || p.Prune(s, result, classFeature) {
		return nil
	}
	return result
}

/*
newRangePartition returns the partition of the given range in 2 parts that generates the most information gain
*/
func newRangePartition(s Set, f *ContinuousFeature, classFeature Feature, entropy, a, b float64) *Partition {
	var floatValues []float64
	for _, v := range s.FeatureValues(f) {
		vf, _ := v.(float64)
		floatValues = append(floatValues, vf)
	}
	if len(floatValues) < 2 {
		return nil
	}
	sort.Float64s(floatValues)
	var result *Partition
	for i, vf := range floatValues[1:] {
		threshold := (floatValues[i] + vf) / 2.0
		subtrees := []*Tree{
			NewTreeFromFeatureCriterion(NewContinuousFeatureCriterion(f, a, threshold), s),
			NewTreeFromFeatureCriterion(NewContinuousFeatureCriterion(f, threshold, b), s),
		}
		informationGain := entropy
		totalCount := float64(s.Count())
		for _, subtree := range subtrees {
			informationGain -= subtree.set.Entropy(classFeature) * float64(subtree.set.Count()) / totalCount
		}
		if result == nil || result.informationGain < informationGain {
			result = &Partition{f, subtrees, informationGain}
		}
	}
	return result
}

func newContinuousPartition(s Set, f *ContinuousFeature, classFeature Feature, entropy, a, b float64, p Pruner) *Partition {
	initialPartition := newRangePartition(s, f, classFeature, entropy, a, b)
	if initialPartition == nil || p.Prune(s, initialPartition, classFeature) {
		return nil
	}
	var resultSubtrees []*Tree
	informationGain := entropy
	totalCount := float64(s.Count())
	for _, subtree := range initialPartition.subtrees {
		fc, _ := subtree.featureCriterion.(ContinuousFeatureCriterion)
		a, b := fc.Interval()
		subsetEntropy := subtree.set.Entropy(classFeature)
		subpartition := newContinuousPartition(subtree.set, f, classFeature, subsetEntropy, a, b, p)
		if subpartition == nil {
			resultSubtrees = append(resultSubtrees, subtree)
			informationGain -= subsetEntropy * float64(subtree.set.Count()) / totalCount
		} else {
			for _, st := range subpartition.subtrees {
				resultSubtrees = append(resultSubtrees, st)
				informationGain -= st.set.Entropy(classFeature) * float64(st.set.Count()) / totalCount
			}
		}
	}
	return &Partition{f, resultSubtrees, informationGain}
}
