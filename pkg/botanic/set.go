package botanic

import (
	"fmt"
	"math"
)

const (
	sampleCountThresholdForSetImplementation = 100
)

/*
Set represents a collection of samples.

Its Entropy method returns the entropy of the set: a measure of the
disinformation we have on the classes of samples that belong to it.

Its Classes method returns the set of uniq classes of samples belonging to the
set.

Its SubsetWith method takes a FeatureCriterion and returns a subset that only
contains samples that satisfy it.

Its Samples method returns the samples it contains
*/
type Set interface {
	Entropy(Feature) (float64, error)
	SubsetWith(FeatureCriterion) (Set, error)
	FeatureValues(Feature) ([]interface{}, error)
	CountFeatureValues(Feature) (map[string]int, error)
	Samples() ([]Sample, error)
	Count() (int, error)
}

type memoryIntensiveSubsettingSet struct {
	samples []Sample
}

type cpuIntensiveSubsettingSet struct {
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
		return &cpuIntensiveSubsettingSet{samples, []FeatureCriterion{}}
	}
	return &memoryIntensiveSubsettingSet{samples}
}

/*
NewMemoryIntensiveSet takes a slice of samples and returns a Set
built with them. A memory-intensive set is an implementation that
replicates the slice of samples when subsetting to reduce
calculations at the cost of increased memory.
*/
func NewMemoryIntensiveSet(samples []Sample) Set {
	return &memoryIntensiveSubsettingSet{samples}
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
	return &cpuIntensiveSubsettingSet{samples, []FeatureCriterion{}}
}

func (s *memoryIntensiveSubsettingSet) Count() (int, error) {
	return len(s.samples), nil
}

func (s *cpuIntensiveSubsettingSet) Count() (int, error) {
	var length int
	s.iterateOnSet(func(_ Sample) (bool, error) {
		length++
		return true, nil
	})
	return length, nil
}

func (s *memoryIntensiveSubsettingSet) Entropy(f Feature) (float64, error) {
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	for _, sample := range s.samples {
		v, err := sample.ValueFor(f)
		if err != nil {
			return result, err
		}
		if v != nil {
			vString := fmt.Sprintf("%v", v)
			count += 1.0
			featureValueCounts[vString] += 1.0
		}
	}
	for _, v := range featureValueCounts {
		probValue := v / count
		result -= probValue * math.Log(probValue)
	}
	return result, nil
}

func (s *cpuIntensiveSubsettingSet) Entropy(f Feature) (float64, error) {
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	err := s.iterateOnSet(func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(f)
		if err != nil {
			return false, err
		}
		if v != nil {
			vString := fmt.Sprintf("%v", v)
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
	return result, nil
}

func (s *memoryIntensiveSubsettingSet) FeatureValues(f Feature) ([]interface{}, error) {
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

func (s *cpuIntensiveSubsettingSet) FeatureValues(f Feature) ([]interface{}, error) {
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

func (s *memoryIntensiveSubsettingSet) SubsetWith(fc FeatureCriterion) (Set, error) {
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
	return &memoryIntensiveSubsettingSet{samples}, nil
}

func (s *cpuIntensiveSubsettingSet) SubsetWith(fc FeatureCriterion) (Set, error) {
	criteria := []FeatureCriterion{fc}
	criteria = append(criteria, s.criteria...)
	return &cpuIntensiveSubsettingSet{s.samples, criteria}, nil
}

func (s *memoryIntensiveSubsettingSet) Samples() ([]Sample, error) {
	return s.samples, nil
}

func (s *cpuIntensiveSubsettingSet) Samples() ([]Sample, error) {
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

func (s *memoryIntensiveSubsettingSet) CountFeatureValues(f Feature) (map[string]int, error) {
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

func (s *cpuIntensiveSubsettingSet) CountFeatureValues(f Feature) (map[string]int, error) {
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
	count, _ := s.Count()
	return fmt.Sprintf("[ %v ]", count)
}

func (s *cpuIntensiveSubsettingSet) String() string {
	count, _ := s.Count()
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
