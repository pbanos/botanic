package dbdataset

import (
	"context"
	"fmt"
	"math"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

/*
Set is a dataset.Dataset to which samples can be added

Its AddSample takes a dataset.Sample and adds it to the dataset,
returning an error if any errors occur or nil otherwise.
*/
type Set interface {
	dataset.Dataset
	Write(context.Context, []dataset.Sample) (int, error)
	Read(context.Context) (<-chan dataset.Sample, <-chan error)
}

type dbSet struct {
	db                    Adapter
	features              []feature.Feature
	criteria              []*FeatureCriterion
	featureNamesColumns   map[string]string
	columnFeatures        map[string]feature.Feature
	discreteValues        map[int]string
	inverseDiscreteValues map[string]int
	dfColumns             []string
	cfColumns             []string
	count                 *int
	entropy               *float64
}

/*
Open takes an Adapter to a db backend and a slice of feature.Feature
and returns a Set backed by the given adapter or an error if no dataset is
available through the given adapter.

This function expects the adapter to have the samples and discrete value
tables already created, and the discrete value table initialized with all
the values of the discrete features in the features slice.
*/
func Open(ctx context.Context, dbAdapter Adapter, features []feature.Feature) (Set, error) {
	ss := &dbSet{db: dbAdapter, features: features}
	err := ss.initFeatureColumns()
	if err != nil {
		return nil, err
	}
	err = ss.init(ctx)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

/*
Create takes an Adapter and a slice of feature.Feature and returns a Set
backed by the given adapter or an error.

This function will ensure that the samples and discrete value tables are
created on the database, and that the discrete value table has all the
values for the discrete features on the features slice.
*/
func Create(ctx context.Context, dbAdapter Adapter, features []feature.Feature) (Set, error) {
	ss := &dbSet{db: dbAdapter, features: features}
	err := ss.initFeatureColumns()
	if err != nil {
		return nil, err
	}
	err = ss.initDB(ctx)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (ss *dbSet) Count(ctx context.Context) (int, error) {
	if ss.count != nil {
		return *ss.count, nil
	}
	result, err := ss.db.CountSamples(ctx, ss.criteria)
	if err == nil {
		ss.count = &result
	}
	return result, err
}

func (ss *dbSet) Entropy(ctx context.Context, f feature.Feature) (float64, error) {
	if ss.entropy != nil {
		return *ss.entropy, nil
	}
	var result, count float64
	column, ok := ss.featureNamesColumns[f.Name()]
	if !ok {
		return 0.0, fmt.Errorf("unknown feature %s", f.Name())
	}
	if _, ok = f.(*feature.DiscreteFeature); ok {
		featureValueCounts, err := ss.db.CountSampleDiscreteFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return 0.0, err
		}
		for _, c := range featureValueCounts {
			count += float64(c)
		}
		for _, c := range featureValueCounts {
			probValue := float64(c) / count
			result -= probValue * math.Log(probValue)
		}
	} else {
		featureValueCounts, err := ss.db.CountSampleContinuousFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return 0.0, err
		}
		for _, c := range featureValueCounts {
			count += float64(c)
		}
		for _, c := range featureValueCounts {
			probValue := float64(c) / count
			result -= probValue * math.Log(probValue)
		}
	}
	ss.entropy = &result
	return result, nil
}

func (ss *dbSet) FeatureValues(ctx context.Context, f feature.Feature) ([]interface{}, error) {
	var err error
	var result []interface{}
	column, ok := ss.featureNamesColumns[f.Name()]
	if !ok {
		return nil, fmt.Errorf("unknown feature %s", f.Name())
	}
	if _, ok = f.(*feature.DiscreteFeature); ok {
		var values []int
		values, err = ss.db.ListSampleDiscreteFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			result = append(result, v)
		}
	} else {
		var values []float64
		values, err = ss.db.ListSampleContinuousFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return nil, err
		}
		for _, v := range values {
			result = append(result, v)
		}
	}
	return result, nil
}

func (ss *dbSet) Samples(ctx context.Context) ([]dataset.Sample, error) {
	rawSamples, err := ss.db.ListSamples(ctx, ss.criteria, ss.dfColumns, ss.cfColumns)
	if err != nil {
		return nil, err
	}
	samples := make([]dataset.Sample, 0, len(rawSamples))
	for _, s := range rawSamples {
		samples = append(samples, &Sample{Values: s, DiscreteFeatureValues: ss.discreteValues, FeatureNamesColumns: ss.featureNamesColumns})
	}
	return samples, nil
}

func (ss *dbSet) SubsetWith(ctx context.Context, fc feature.Criterion) (dataset.Dataset, error) {
	rfc, err := NewFeatureCriteria(fc, ss.db.ColumnName, ss.inverseDiscreteValues)
	if err != nil {
		return nil, err
	}
	subsetCriteria := make([]*FeatureCriterion, 0, len(ss.criteria)+len(rfc))
	subsetCriteria = append(subsetCriteria, ss.criteria...)
	subsetCriteria = append(subsetCriteria, rfc...)
	return &dbSet{
		db:                    ss.db,
		features:              ss.features,
		criteria:              subsetCriteria,
		discreteValues:        ss.discreteValues,
		inverseDiscreteValues: ss.inverseDiscreteValues,
		featureNamesColumns:   ss.featureNamesColumns,
		columnFeatures:        ss.columnFeatures,
		dfColumns:             ss.dfColumns,
		cfColumns:             ss.cfColumns,
	}, nil
}

func (ss *dbSet) CountFeatureValues(ctx context.Context, f feature.Feature) (map[string]int, error) {
	result := make(map[string]int)
	column, ok := ss.featureNamesColumns[f.Name()]
	if !ok {
		return nil, fmt.Errorf("unknown feature %s", f.Name())
	}
	if _, ok = f.(*feature.DiscreteFeature); ok {
		featureValueCounts, err := ss.db.CountSampleDiscreteFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return nil, err
		}
		for k, v := range featureValueCounts {
			result[ss.discreteValues[k]] = v
		}
	} else {
		featureValueCounts, err := ss.db.CountSampleContinuousFeatureValues(ctx, column, ss.criteria)
		if err != nil {
			return nil, err
		}
		for k, v := range featureValueCounts {
			result[fmt.Sprintf("%f", k)] = v
		}
	}
	return result, nil
}

func (ss *dbSet) Write(ctx context.Context, samples []dataset.Sample) (int, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	rawSamples := make([]map[string]interface{}, 0, len(samples))
	for _, s := range samples {
		rs, err := ss.newRawSample(s)
		if err != nil {
			return 0, err
		}
		rawSamples = append(rawSamples, rs)
	}
	return ss.db.AddSamples(ctx, rawSamples, ss.dfColumns, ss.cfColumns)
}

func (ss *dbSet) Read(ctx context.Context) (<-chan dataset.Sample, <-chan error) {
	sampleStream := make(chan dataset.Sample)
	errStream := make(chan error)
	go func() {
		err := ss.db.IterateOnSamples(
			ctx,
			ss.criteria,
			ss.dfColumns,
			ss.cfColumns,
			func(n int, rs map[string]interface{}) (bool, error) {
				s := &Sample{
					Values:                rs,
					DiscreteFeatureValues: ss.discreteValues,
					FeatureNamesColumns:   ss.featureNamesColumns}
				select {
				case <-ctx.Done():
					return false, nil
				case sampleStream <- s:
				}
				return true, nil
			})
		if err != nil {
			go func() {
				errStream <- err
				close(errStream)
			}()
		} else {
			close(errStream)
		}
		close(sampleStream)
	}()
	return sampleStream, errStream
}

func (ss *dbSet) initDB(ctx context.Context) error {
	err := ss.db.CreateDiscreteValuesTable(ctx)
	if err != nil {
		return err
	}
	err = ss.db.CreateSampleTable(ctx, ss.dfColumns, ss.cfColumns)
	if err != nil {
		return err
	}
	ss.discreteValues, err = ss.db.ListDiscreteValues(ctx)
	if err != nil {
		return err
	}
	newValues := ss.unavailableDiscreteValues()
	_, err = ss.db.AddDiscreteValues(ctx, newValues)
	if err != nil {
		return err
	}
	err = ss.init(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (ss *dbSet) unavailableDiscreteValues() []string {
	var unavailableDiscreteValues []string
	for _, f := range ss.features {
		df, ok := f.(*feature.DiscreteFeature)
		if ok {
			for _, fv := range df.AvailableValues() {
				var present bool
				for _, pv := range ss.discreteValues {
					if fv == pv {
						present = true
						break
					}
				}
				if !present {
					for _, uv := range unavailableDiscreteValues {
						if fv == uv {
							present = true
							break
						}
					}
					if !present {
						unavailableDiscreteValues = append(unavailableDiscreteValues, fv)
					}
				}
			}
		}
	}
	return unavailableDiscreteValues
}

func (ss *dbSet) init(ctx context.Context) error {
	var err error
	ss.discreteValues, err = ss.db.ListDiscreteValues(ctx)
	if err != nil {
		return err
	}
	ss.inverseDiscreteValues = make(map[string]int)
	for k, v := range ss.discreteValues {
		ss.inverseDiscreteValues[v] = k
	}
	return nil
}

func (ss *dbSet) newRawSample(s dataset.Sample) (map[string]interface{}, error) {
	rs := make(map[string]interface{})
	for _, f := range ss.features {
		v, err := s.ValueFor(f)
		if err != nil {
			return nil, err
		}
		if v != nil {
			_, ok := f.(*feature.DiscreteFeature)
			if ok {
				vs, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("expected string value for discrete feature %s of sample, got %T", f.Name(), v)
				}
				v, ok = ss.inverseDiscreteValues[vs]
			}
			rs[f.Name()] = v
		}
	}
	return rs, nil
}

func (ss *dbSet) initFeatureColumns() error {
	ss.columnFeatures = make(map[string]feature.Feature)
	ss.featureNamesColumns = make(map[string]string)
	for _, f := range ss.features {
		column, err := ss.db.ColumnName(f.Name())
		if err != nil {
			return fmt.Errorf("invalid feature %s: %v", f.Name(), err)
		}
		of, ok := ss.columnFeatures[column]
		if ok {
			return fmt.Errorf("%s and %s feature names translate to the same column name %s", f.Name(), of.Name(), column)
		}
		ss.columnFeatures[column] = f
		ss.featureNamesColumns[f.Name()] = column
	}
	for _, f := range ss.features {
		if _, ok := f.(*feature.DiscreteFeature); ok {
			ss.dfColumns = append(ss.dfColumns, ss.featureNamesColumns[f.Name()])
		} else {
			ss.cfColumns = append(ss.cfColumns, ss.featureNamesColumns[f.Name()])
		}
	}
	return nil
}
