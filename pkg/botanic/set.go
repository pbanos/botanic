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
	Entropy(Feature) float64
	SubsetWith(FeatureCriterion) Set
	FeatureValues(Feature) []interface{}
	Samples() []Sample
	Count() int
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

func (s *memoryIntensiveSubsettingSet) Count() int {
	return len(s.samples)
}

func (s *cpuIntensiveSubsettingSet) Count() int {
	var length int
	s.iterateOnSet(func(_ Sample) bool {
		length++
		return true
	})
	return length
}

func (s *memoryIntensiveSubsettingSet) Entropy(f Feature) float64 {
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	for _, sample := range s.samples {
		if v := sample.ValueFor(f); v != nil {
			vString := fmt.Sprintf("%v", v)
			count += 1.0
			featureValueCounts[vString] += 1.0
		}
	}
	for _, v := range featureValueCounts {
		probValue := v / count
		result -= probValue * math.Log(probValue)
	}
	return result
}

func (s *cpuIntensiveSubsettingSet) Entropy(f Feature) float64 {
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	s.iterateOnSet(func(sample Sample) bool {
		if v := sample.ValueFor(f); v != nil {
			vString := fmt.Sprintf("%v", v)
			count += 1.0
			featureValueCounts[vString] += 1.0
		}
		return true
	})
	for _, v := range featureValueCounts {
		probValue := v / count
		result -= probValue * math.Log(probValue)
	}
	return result
}

func (s *memoryIntensiveSubsettingSet) FeatureValues(f Feature) []interface{} {
	result := []interface{}{}
	encountered := make(map[string]bool)
	for _, sample := range s.samples {
		v := sample.ValueFor(f)
		vString := fmt.Sprintf("%v", v)
		if !encountered[vString] {
			encountered[vString] = true
			result = append(result, v)
		}
	}
	return result
}

func (s *cpuIntensiveSubsettingSet) FeatureValues(f Feature) []interface{} {
	result := []interface{}{}
	encountered := make(map[string]bool)
	s.iterateOnSet(func(sample Sample) bool {
		v := sample.ValueFor(f)
		vString := fmt.Sprintf("%v", v)
		if !encountered[vString] {
			encountered[vString] = true
			result = append(result, v)
		}
		return true
	})
	return result
}

func (s *memoryIntensiveSubsettingSet) SubsetWith(fc FeatureCriterion) Set {
	var samples []Sample
	for _, sample := range s.samples {
		if fc.SatisfiedBy(sample) {
			samples = append(samples, sample)
		}
	}
	return &memoryIntensiveSubsettingSet{samples}
}

func (s *cpuIntensiveSubsettingSet) SubsetWith(fc FeatureCriterion) Set {
	criteria := []FeatureCriterion{fc}
	criteria = append(criteria, s.criteria...)
	return &cpuIntensiveSubsettingSet{s.samples, criteria}
}

func (s *memoryIntensiveSubsettingSet) Samples() []Sample {
	return s.samples
}

func (s *cpuIntensiveSubsettingSet) Samples() []Sample {
	var samples []Sample
	s.iterateOnSet(func(sample Sample) bool {
		samples = append(samples, sample)
		return true
	})
	return samples
}

func (s *memoryIntensiveSubsettingSet) String() string {
	return fmt.Sprintf("[ %v ]", s.Count())
}

func (s *cpuIntensiveSubsettingSet) String() string {
	return fmt.Sprintf("[ %v ]", s.Count())
}

func (s *cpuIntensiveSubsettingSet) iterateOnSet(lambda func(Sample) bool) {
	for _, sample := range s.samples {
		skip := false
		for _, criterion := range s.criteria {
			if !criterion.SatisfiedBy(sample) {
				skip = true
				break
			}
		}
		if !skip {
			if !lambda(sample) {
				break
			}
		}
	}
}
