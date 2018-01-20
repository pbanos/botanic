package botanic

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/queue"
	"github.com/pbanos/botanic/set"
	"github.com/pbanos/botanic/tree"
)

/*
Partition represents a partition of a set according to a feature
into subtrees with an information gain to predict the class feature
*/
type Partition struct {
	Feature         feature.Feature
	Tasks           []*queue.Task
	informationGain float64
}

/*
NewDiscretePartition takes a context.Context, a set, a discrete feature and a class
feature and returns a partition of the set for the given feature. The result may be
nil if the obtained information gain is considered insufficient
*/
func NewDiscretePartition(ctx context.Context, s set.Set, f *feature.DiscreteFeature, classFeature feature.Feature, p Pruner) (*Partition, error) {
	availableValues := f.AvailableValues()
	tasks := make([]*queue.Task, 0, len(availableValues)+1)
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
		n := &tree.Node{FeatureCriterion: feature.NewDiscreteCriterion(f, value)}
		ns, err := s.SubsetWith(ctx, n.FeatureCriterion)
		if err != nil {
			return nil, err
		}
		task := &queue.Task{
			Node: n,
			Set:  ns,
		}
		tasks = append(tasks, task)
		nEntropy, err := ns.Entropy(ctx, classFeature)
		if err != nil {
			return nil, err
		}
		subtreeCount, err := ns.Count(ctx)
		if err != nil {
			return nil, err
		}
		informationGain -= nEntropy * float64(subtreeCount) / totalCount
	}
	result := &Partition{f, tasks, informationGain}
	ok, err := p.Prune(ctx, s, result, classFeature)
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	task := &queue.Task{
		Node: &tree.Node{FeatureCriterion: feature.NewUndefinedCriterion(f)},
		Set:  s,
	}
	result.Tasks = append(result.Tasks, task)
	return result, nil
}

/*
NewContinuousPartition takes a context.Context, a set, a continuous feature and
a class feature and returns a partition of the set for the given feature. The
result may be nil if the obtained information gain is considered insufficient
*/
func NewContinuousPartition(ctx context.Context, s set.Set, f *feature.ContinuousFeature, classFeature feature.Feature, p Pruner) (*Partition, error) {
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
	task := &queue.Task{
		Node: &tree.Node{FeatureCriterion: feature.NewUndefinedCriterion(f)},
		Set:  s,
	}
	result.Tasks = append(result.Tasks, task)
	return result, nil
}

func partition(ctx context.Context, s set.Set, f feature.Feature, cf feature.Feature, p Pruner) (*Partition, error) {
	switch f := f.(type) {
	default:
		return nil, fmt.Errorf("unknown feature type %T for feature %v", f, f.Name())
	case *feature.DiscreteFeature:
		return NewDiscretePartition(ctx, s, f, cf, p)
	case *feature.ContinuousFeature:
		return NewContinuousPartition(ctx, s, f, cf, p)
	}
}

/*
newRangePartition returns the partition of the given range in 2 parts that generates the most information gain
*/
func newRangePartition(ctx context.Context, s set.Set, f *feature.ContinuousFeature, classFeature feature.Feature, entropy, a, b float64) (*Partition, error) {
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

		n := &tree.Node{FeatureCriterion: feature.NewContinuousCriterion(f, a, threshold)}
		ns, err := s.SubsetWith(ctx, n.FeatureCriterion)
		if err != nil {
			return nil, err
		}
		t1 := &queue.Task{
			Node: n,
			Set:  ns,
		}

		n = &tree.Node{FeatureCriterion: feature.NewContinuousCriterion(f, threshold, b)}
		ns, err = s.SubsetWith(ctx, n.FeatureCriterion)
		if err != nil {
			return nil, err
		}
		t2 := &queue.Task{
			Node: n,
			Set:  ns,
		}
		tasks := []*queue.Task{t1, t2}
		informationGain := entropy
		count, err := s.Count(ctx)
		if err != nil {
			return nil, err
		}
		totalCount := float64(count)
		for _, task := range tasks {
			taskEntropy, err := task.Set.Entropy(ctx, classFeature)
			if err != nil {
				return nil, err
			}
			taskCount, err := task.Set.Count(ctx)
			if err != nil {
				return nil, err
			}
			informationGain -= taskEntropy * float64(taskCount) / totalCount
		}
		if result == nil || result.informationGain < informationGain {
			result = &Partition{f, tasks, informationGain}
		}
	}
	return result, nil
}

/*
newContinuousPartition takes a context.Context, a set, a continuous feature,
a class Feature, the entropy of the given set, an range of float64 numbers
a-b and a pruner and returns a partition of the set for the given range or
an error.
The partition is built using newRangePartition to split the range into 2 ranges
and then recursively call itself until the range can no longer be splitted or
the pruner prunes the obtained range partition.
*/
func newContinuousPartition(ctx context.Context, s set.Set, f *feature.ContinuousFeature, classFeature feature.Feature, entropy, a, b float64, p Pruner) (*Partition, error) {
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
	var resultTasks []*queue.Task
	informationGain := entropy
	count, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}
	totalCount := float64(count)
	for _, task := range initialPartition.Tasks {
		fc, _ := task.Node.FeatureCriterion.(feature.ContinuousCriterion)
		a, b := fc.Interval()
		subsetEntropy, err := task.Set.Entropy(ctx, classFeature)
		if err != nil {
			return nil, err
		}
		subpartition, err := newContinuousPartition(ctx, task.Set, f, classFeature, subsetEntropy, a, b, p)
		if err != nil {
			return nil, err
		}
		if subpartition == nil {
			taskCount, err := task.Set.Count(ctx)
			if err != nil {
				return nil, err
			}
			resultTasks = append(resultTasks, task)
			informationGain -= subsetEntropy * float64(taskCount) / totalCount
		} else {
			for _, st := range subpartition.Tasks {
				stEntropy, err := st.Set.Entropy(ctx, classFeature)
				if err != nil {
					return nil, err
				}
				stCount, err := st.Set.Count(ctx)
				if err != nil {
					return nil, err
				}
				informationGain -= stEntropy * float64(stCount) / totalCount
				resultTasks = append(resultTasks, st)
			}
		}
	}
	return &Partition{f, resultTasks, informationGain}, nil
}
