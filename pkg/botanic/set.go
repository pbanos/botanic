package botanic

import (
	"context"
	"fmt"
	"math"
)

const (
	sampleCountThresholdForSetImplementation = 100
)

/*
Set represents a collection of samples.

Its Entropy method returns the entropy of the set for a given Feature: a
measure of the disinformation we have on the classes of samples that belong to
it.

Its Classes method returns the set of uniq classes of samples belonging to the
set.

Its SubsetWith method takes a FeatureCriterion and returns a subset that only
contains samples that satisfy it.

Its Samples method returns the samples it contains
*/
type Set interface {
	Entropy(context.Context, Feature) (float64, error)
	SubsetWith(context.Context, FeatureCriterion) (Set, error)
	FeatureValues(context.Context, Feature) ([]interface{}, error)
	CountFeatureValues(context.Context, Feature) (map[string]int, error)
	Samples(context.Context) ([]Sample, error)
	Count(context.Context) (int, error)
}

type memoryIntensiveSubsettingSet struct {
	entropy *float64
	samples []Sample
}

type cpuIntensiveSubsettingSet struct {
	entropy  *float64
	count    *int
	samples  []Sample
	criteria []FeatureCriterion
}

/*
NewSet takes a slice of samples and returns a set built with them.
The set will be a CPU intensive one when the number of samples is
over sampleCountThresholdForSetImplementation
*/
func NewSet(samples []Sample) Set {
	if len(samples) > sampleCountThresholdForSetImplementation {
		return &cpuIntensiveSubsettingSet{nil, nil, samples, []FeatureCriterion{}}
	}
	return &memoryIntensiveSubsettingSet{nil, samples}
}

/*
NewMemoryIntensiveSet takes a slice of samples and returns a Set
built with them. A memory-intensive set is an implementation that
replicates the slice of samples when subsetting to reduce
calculations at the cost of increased memory.
*/
func NewMemoryIntensiveSet(samples []Sample) Set {
	return &memoryIntensiveSubsettingSet{nil, samples}
}

/*
NewCPUIntensiveSet takes a slice of samples and returns a Set
built with them. A cpu-intensive set is an implementation that
instead of replicating the samples when subsetting, stores the
applying feature criteria to define the subset and keeps the same
sample slice. This can achieve a drastic reduction in memory use
that comes at the cost of CPU time: every calculation that goes over
the samples of the set will apply the feature criteria of the set
on all original samples (the ones provided to this method).
*/
func NewCPUIntensiveSet(samples []Sample) Set {
	return &cpuIntensiveSubsettingSet{nil, nil, samples, []FeatureCriterion{}}
}

func (s *memoryIntensiveSubsettingSet) Count(ctx context.Context) (int, error) {
	return len(s.samples), nil
}

func (s *cpuIntensiveSubsettingSet) Count(ctx context.Context) (int, error) {
	if s.count != nil {
		return *s.count, nil
	}
	var length int
	s.iterateOnSet(func(_ Sample) (bool, error) {
		length++
		return true, nil
	})
	s.count = &length
	return length, nil
}

func (s *memoryIntensiveSubsettingSet) Entropy(ctx context.Context, f Feature) (float64, error) {
	if s.entropy != nil {
		return *s.entropy, nil
	}
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	for _, sample := range s.samples {
		v, err := sample.ValueFor(f)
		if err != nil {
			return result, err
		}
		if v != nil {
			vString, ok := v.(string)
			if !ok {
				vString = fmt.Sprintf("%v", v)
			}
			count += 1.0
			featureValueCounts[vString] += 1.0
		}
	}
	for _, v := range featureValueCounts {
		probValue := v / count
		result -= probValue * math.Log(probValue)
	}
	s.entropy = &result
	return result, nil
}

func (s *cpuIntensiveSubsettingSet) Entropy(ctx context.Context, f Feature) (float64, error) {
	if s.entropy != nil {
		return *s.entropy, nil
	}
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	err := s.iterateOnSet(func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(f)
		if err != nil {
			return false, err
		}
		if v != nil {
			vString, ok := v.(string)
			if !ok {
				vString = fmt.Sprintf("%v", v)
			}
			count += 1.0
			featureValueCounts[vString] += 1.0
		}
		return true, nil
	})
	if err != nil {
		return result, err
	}
	for _, v := range featureValueCounts {
		probValue := v / count
		result -= probValue * math.Log(probValue)
	}
	s.entropy = &result
	return result, nil
}

func (s *memoryIntensiveSubsettingSet) FeatureValues(ctx context.Context, f Feature) ([]interface{}, error) {
	result := []interface{}{}
	encountered := make(map[string]bool)
	for _, sample := range s.samples {
		v, err := sample.ValueFor(f)
		if err != nil {
			return nil, err
		}
		vString := fmt.Sprintf("%v", v)
		if !encountered[vString] {
			encountered[vString] = true
			result = append(result, v)
		}
	}
	return result, nil
}

func (s *cpuIntensiveSubsettingSet) FeatureValues(ctx context.Context, f Feature) ([]interface{}, error) {
	result := []interface{}{}
	encountered := make(map[string]bool)
	err := s.iterateOnSet(func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(f)
		if err != nil {
			return false, err
		}
		vString := fmt.Sprintf("%v", v)
		if !encountered[vString] {
			encountered[vString] = true
			result = append(result, v)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *memoryIntensiveSubsettingSet) SubsetWith(ctx context.Context, fc FeatureCriterion) (Set, error) {
	var samples []Sample
	for _, sample := range s.samples {
		ok, err := fc.SatisfiedBy(sample)
		if err != nil {
			return nil, err
		}
		if ok {
			samples = append(samples, sample)
		}
	}
	return &memoryIntensiveSubsettingSet{nil, samples}, nil
}

func (s *cpuIntensiveSubsettingSet) SubsetWith(ctx context.Context, fc FeatureCriterion) (Set, error) {
	criteria := []FeatureCriterion{fc}
	criteria = append(criteria, s.criteria...)
	return &cpuIntensiveSubsettingSet{nil, nil, s.samples, criteria}, nil
}

func (s *memoryIntensiveSubsettingSet) Samples(ctx context.Context) ([]Sample, error) {
	return s.samples, nil
}

func (s *cpuIntensiveSubsettingSet) Samples(ctx context.Context) ([]Sample, error) {
	var samples []Sample
	err := s.iterateOnSet(func(sample Sample) (bool, error) {
		samples = append(samples, sample)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return samples, nil
}

func (s *memoryIntensiveSubsettingSet) CountFeatureValues(ctx context.Context, f Feature) (map[string]int, error) {
	result := make(map[string]int)
	for _, sample := range s.samples {
		v, err := sample.ValueFor(f)
		if err != nil {
			return nil, err
		}
		vString := fmt.Sprintf("%v", v)
		result[vString]++
	}
	return result, nil
}

func (s *cpuIntensiveSubsettingSet) CountFeatureValues(ctx context.Context, f Feature) (map[string]int, error) {
	result := make(map[string]int)
	err := s.iterateOnSet(func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(f)
		if err != nil {
			return false, err
		}
		vString := fmt.Sprintf("%v", v)
		result[vString]++
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *memoryIntensiveSubsettingSet) String() string {
	count, _ := s.Count(context.TODO())
	return fmt.Sprintf("[ %v ]", count)
}

func (s *cpuIntensiveSubsettingSet) String() string {
	count, _ := s.Count(context.TODO())
	return fmt.Sprintf("[ %v ]", count)
}

func (s *cpuIntensiveSubsettingSet) iterateOnSet(lambda func(Sample) (bool, error)) error {
	for _, sample := range s.samples {
		skip := false
		for _, criterion := range s.criteria {
			ok, err := criterion.SatisfiedBy(sample)
			if err != nil {
				return err
			}
			if !ok {
				skip = true
				break
			}
		}
		if !skip {
			ok, err := lambda(sample)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
		}
	}
	return nil
}
