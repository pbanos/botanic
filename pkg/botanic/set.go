package botanic

import (
	"fmt"
	"math"
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

type set struct {
	samples []Sample
}

/*
NewSet takes a slice of samples and returns a set built with them
*/
func NewSet(samples []Sample) Set {
	return &set{samples}
}

func (s *set) Count() int {
	return len(s.samples)
}

func (s *set) Entropy(f Feature) float64 {
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

func (s *set) FeatureValues(f Feature) []interface{} {
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

func (s *set) SubsetWith(fc FeatureCriterion) Set {
	var samples []Sample
	for _, sample := range s.samples {
		if fc.SatisfiedBy(sample) {
			samples = append(samples, sample)
		}
	}
	return &set{samples}
}

func (s *set) Samples() []Sample {
	return s.samples
}

func (s *set) String() string {
	return fmt.Sprintf("[ %v ]", s.Count())
}
