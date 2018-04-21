package dataset

import (
	"context"
	"fmt"
	"math"

	"github.com/pbanos/botanic/feature"
)

const (
	sampleCountThresholdForDatasetImplementation = 1000
)

/*
Dataset represents a collection of samples.

Its Entropy method returns the entropy of the dataset for a given Feature: a
measure of the disinformation we have on the classes of samples that belong to
it.

Its Classes method returns the set of uniq classes of samples belonging to the
set.

Its SubsetWith method takes a feature.Criterion and returns a subset that only
contains samples that satisfy it.

Its Samples method returns the samples it contains
*/
type Dataset interface {
	Entropy(context.Context, feature.Feature) (float64, error)
	SubsetWith(context.Context, feature.Criterion) (Dataset, error)
	FeatureValues(context.Context, feature.Feature) ([]interface{}, error)
	CountFeatureValues(context.Context, feature.Feature) (map[string]int, error)
	Samples(context.Context) ([]Sample, error)
	Count(context.Context) (int, error)
	Criteria(context.Context) ([]feature.Criterion, error)
}

type memoryIntensiveSubsettingDataset struct {
	entropy  *float64
	samples  []Sample
	criteria []feature.Criterion
}

type cpuIntensiveSubsettingDataset struct {
	entropy  *float64
	count    *int
	samples  []Sample
	criteria []feature.Criterion
}

/*
New takes a slice of samples and returns a dataset built with them.
The dataset will be a CPU intensive one when the number of samples is
over sampleCountThresholdForDatasetImplementation
*/
func New(samples []Sample) Dataset {
	if len(samples) > sampleCountThresholdForDatasetImplementation {
		return &cpuIntensiveSubsettingDataset{nil, nil, samples, []feature.Criterion{}}
	}
	return &memoryIntensiveSubsettingDataset{nil, samples, nil}
}

/*
NewMemoryIntensive takes a slice of samples and returns a Dataset
built with them. A memory-intensive dataset is an implementation that
replicates the slice of samples when subsetting to reduce
calculations at the cost of increased memory.
*/
func NewMemoryIntensive(samples []Sample) Dataset {
	return &memoryIntensiveSubsettingDataset{nil, samples, nil}
}

/*
NewCPUIntensive takes a slice of samples and returns a Dataset
built with them. A cpu-intensive dataset is an implementation that
instead of replicating the samples when subsetting, stores the
applying feature criteria to define the subset and keeps the same
sample slice. This can achieve a drastic reduction in memory use
that comes at the cost of CPU time: every calculation that goes over
the samples of the dataset will apply the feature criteria of the dataset
on all original samples (the ones provided to this method).
*/
func NewCPUIntensive(samples []Sample) Dataset {
	return &cpuIntensiveSubsettingDataset{nil, nil, samples, []feature.Criterion{}}
}

func (s *memoryIntensiveSubsettingDataset) Count(ctx context.Context) (int, error) {
	return len(s.samples), nil
}

func (s *cpuIntensiveSubsettingDataset) Count(ctx context.Context) (int, error) {
	if s.count != nil {
		return *s.count, nil
	}
	var length int
	s.iterateOnDataset(ctx, func(_ Sample) (bool, error) {
		length++
		return true, nil
	})
	s.count = &length
	return length, nil
}

func (s *memoryIntensiveSubsettingDataset) Entropy(ctx context.Context, f feature.Feature) (float64, error) {
	if s.entropy != nil {
		return *s.entropy, nil
	}
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	for _, sample := range s.samples {
		v, err := sample.ValueFor(ctx, f)
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

func (s *cpuIntensiveSubsettingDataset) Entropy(ctx context.Context, f feature.Feature) (float64, error) {
	if s.entropy != nil {
		return *s.entropy, nil
	}
	var result float64
	featureValueCounts := make(map[string]float64)
	count := 0.0
	err := s.iterateOnDataset(ctx, func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(ctx, f)
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

func (s *memoryIntensiveSubsettingDataset) FeatureValues(ctx context.Context, f feature.Feature) ([]interface{}, error) {
	result := []interface{}{}
	encountered := make(map[string]bool)
	for _, sample := range s.samples {
		v, err := sample.ValueFor(ctx, f)
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

func (s *cpuIntensiveSubsettingDataset) FeatureValues(ctx context.Context, f feature.Feature) ([]interface{}, error) {
	result := []interface{}{}
	encountered := make(map[string]bool)
	err := s.iterateOnDataset(ctx, func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(ctx, f)
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

func (s *memoryIntensiveSubsettingDataset) SubsetWith(ctx context.Context, fc feature.Criterion) (Dataset, error) {
	var samples []Sample
	for _, sample := range s.samples {
		ok, err := fc.SatisfiedBy(ctx, sample)
		if err != nil {
			return nil, err
		}
		if ok {
			samples = append(samples, sample)
		}
	}
	return &memoryIntensiveSubsettingDataset{nil, samples, append([]feature.Criterion{fc}, s.criteria...)}, nil
}

func (s *cpuIntensiveSubsettingDataset) SubsetWith(ctx context.Context, fc feature.Criterion) (Dataset, error) {
	criteria := append([]feature.Criterion{fc}, s.criteria...)
	return &cpuIntensiveSubsettingDataset{nil, nil, s.samples, criteria}, nil
}

func (s *memoryIntensiveSubsettingDataset) Samples(ctx context.Context) ([]Sample, error) {
	return s.samples, nil
}

func (s *cpuIntensiveSubsettingDataset) Samples(ctx context.Context) ([]Sample, error) {
	var samples []Sample
	err := s.iterateOnDataset(ctx, func(sample Sample) (bool, error) {
		samples = append(samples, sample)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return samples, nil
}

func (s *memoryIntensiveSubsettingDataset) CountFeatureValues(ctx context.Context, f feature.Feature) (map[string]int, error) {
	result := make(map[string]int)
	for _, sample := range s.samples {
		v, err := sample.ValueFor(ctx, f)
		if err != nil {
			return nil, err
		}
		vString := fmt.Sprintf("%v", v)
		result[vString]++
	}
	return result, nil
}

func (s *cpuIntensiveSubsettingDataset) CountFeatureValues(ctx context.Context, f feature.Feature) (map[string]int, error) {
	result := make(map[string]int)
	err := s.iterateOnDataset(ctx, func(sample Sample) (bool, error) {
		v, err := sample.ValueFor(ctx, f)
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

func (s *memoryIntensiveSubsettingDataset) Criteria(ctx context.Context) ([]feature.Criterion, error) {
	return s.criteria, nil
}

func (s *cpuIntensiveSubsettingDataset) Criteria(ctx context.Context) ([]feature.Criterion, error) {
	return s.criteria, nil
}

func (s *cpuIntensiveSubsettingDataset) iterateOnDataset(ctx context.Context, lambda func(Sample) (bool, error)) error {
	for _, sample := range s.samples {
		skip := false
		for _, criterion := range s.criteria {
			ok, err := criterion.SatisfiedBy(ctx, sample)
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
