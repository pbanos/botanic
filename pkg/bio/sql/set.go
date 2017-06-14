package sql

import (
	"fmt"
	"math"

	"github.com/pbanos/botanic/pkg/botanic"
)

/*
Set is a botanic.Set to which samples can be added

Its AddSample takes a botanic.Sample and adds it to the set,
returning an error if any errors occur or nil otherwise.
*/
type Set interface {
	botanic.Set
	Write([]botanic.Sample) (int, error)
	Read(done <-chan struct{}) (<-chan botanic.Sample, <-chan error)
}

type sqlSet struct {
	db                    Adapter
	features              []botanic.Feature
	criteria              []*FeatureCriterion
	featureNamesColumns   map[string]string
	columnFeatures        map[string]botanic.Feature
	discreteValues        map[int]string
	inverseDiscreteValues map[string]int
	dfColumns             []string
	cfColumns             []string
}

/*
OpenSet takes an Adapter to a db backend and a slice of botanic.Feature
and returns a Set backed by the given adapter or an error if no set is
available through the given adapter.

This function expects the adapter to have the samples and discrete value
tables already created, and the discrete value table initialized with all
the values of the discrete features in the features slice.
*/
func OpenSet(dbAdapter Adapter, features []botanic.Feature) (Set, error) {
	ss := &sqlSet{db: dbAdapter, features: features}
	err := ss.initFeatureColumns()
	if err != nil {
		return nil, err
	}
	err = ss.init()
	if err != nil {
		return nil, err
	}
	return ss, nil
}

/*
CreateSet takes an Adapter and a slice of botanic.Feature and returns a Set
backed by the given adapter or an error.

This function will ensure that the samples and discrete value tables are
created on the database, and that the discrete value table has all the
values for the discrete features on the features slice.
*/
func CreateSet(dbAdapter Adapter, features []botanic.Feature) (Set, error) {
	ss := &sqlSet{db: dbAdapter, features: features}
	err := ss.initFeatureColumns()
	if err != nil {
		return nil, err
	}
	err = ss.initDB()
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (ss *sqlSet) Count() int {
	count, err := ss.db.CountSamples(ss.criteria)
	if err != nil {
		panic(err)
	}
	return count
}

func (ss *sqlSet) Entropy(f botanic.Feature) float64 {
	var result, count float64
	column, ok := ss.featureNamesColumns[f.Name()]
	if !ok {
		panic(fmt.Errorf("unknown feature %s", f.Name()))
	}
	if _, ok = f.(*botanic.DiscreteFeature); ok {
		featureValueCounts, err := ss.db.CountSampleDiscreteFeatureValues(column, ss.criteria)
		if err != nil {
			panic(err)
		}
		for _, c := range featureValueCounts {
			count += float64(c)
		}
		for _, c := range featureValueCounts {
			probValue := float64(c) / count
			result -= probValue * math.Log(probValue)
		}
	} else {
		featureValueCounts, err := ss.db.CountSampleContinuousFeatureValues(column, ss.criteria)
		if err != nil {
			panic(err)
		}
		for _, c := range featureValueCounts {
			count += float64(c)
		}
		for _, c := range featureValueCounts {
			probValue := float64(c) / count
			result -= probValue * math.Log(probValue)
		}
	}
	return result
}

func (ss *sqlSet) FeatureValues(f botanic.Feature) []interface{} {
	var err error
	var result []interface{}
	column, ok := ss.featureNamesColumns[f.Name()]
	if !ok {
		panic(fmt.Errorf("unknown feature %s", f.Name()))
	}
	if _, ok = f.(*botanic.DiscreteFeature); ok {
		var values []int
		values, err = ss.db.ListSampleDiscreteFeatureValues(column, ss.criteria)
		if err != nil {
			panic(err)
		}
		for _, v := range values {
			result = append(result, v)
		}
	} else {
		var values []float64
		values, err = ss.db.ListSampleContinuousFeatureValues(column, ss.criteria)
		if err != nil {
			panic(err)
		}
		for _, v := range values {
			result = append(result, v)
		}
	}
	return result
}

func (ss *sqlSet) Samples() []botanic.Sample {
	rawSamples, err := ss.db.ListSamples(ss.criteria, ss.dfColumns, ss.cfColumns)
	if err != nil {
		panic(err)
	}
	samples := make([]botanic.Sample, len(rawSamples))
	for _, s := range rawSamples {
		samples = append(samples, &Sample{Values: s, DiscreteFeatureValues: ss.discreteValues, FeatureNamesColumns: ss.featureNamesColumns})
	}
	return samples
}

func (ss *sqlSet) SubsetWith(fc botanic.FeatureCriterion) botanic.Set {
	rfc, err := NewFeatureCriteria(fc, ss.db.ColumnName, ss.inverseDiscreteValues)
	if err != nil {
		panic(err)
	}
	subsetCriteria := make([]*FeatureCriterion, 0, len(ss.criteria)+len(rfc))
	subsetCriteria = append(subsetCriteria, ss.criteria...)
	subsetCriteria = append(subsetCriteria, rfc...)
	return &sqlSet{
		db:                    ss.db,
		features:              ss.features,
		criteria:              subsetCriteria,
		discreteValues:        ss.discreteValues,
		inverseDiscreteValues: ss.inverseDiscreteValues,
		featureNamesColumns:   ss.featureNamesColumns,
		columnFeatures:        ss.columnFeatures,
		dfColumns:             ss.dfColumns,
		cfColumns:             ss.cfColumns,
	}
}

func (ss *sqlSet) Write(samples []botanic.Sample) (int, error) {
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
	return ss.db.AddSamples(rawSamples, ss.dfColumns, ss.cfColumns)
}

func (ss *sqlSet) Read(done <-chan struct{}) (<-chan botanic.Sample, <-chan error) {
	sampleStream := make(chan botanic.Sample)
	errStream := make(chan error)
	go func() {
		err := ss.db.IterateOnSamples(
			ss.criteria,
			ss.dfColumns,
			ss.cfColumns,
			func(n int, rs map[string]interface{}) (bool, error) {
				s := &Sample{
					Values:                rs,
					DiscreteFeatureValues: ss.discreteValues,
					FeatureNamesColumns:   ss.featureNamesColumns}
				select {
				case <-done:
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

func (ss *sqlSet) initDB() error {
	err := ss.db.CreateDiscreteValuesTable()
	if err != nil {
		return err
	}
	err = ss.db.CreateSampleTable(ss.dfColumns, ss.cfColumns)
	if err != nil {
		return err
	}
	ss.discreteValues, err = ss.db.ListDiscreteValues()
	if err != nil {
		return err
	}
	newValues := ss.unavailableDiscreteValues()
	_, err = ss.db.AddDiscreteValues(newValues)
	if err != nil {
		return err
	}
	err = ss.init()
	if err != nil {
		return err
	}
	return nil
}

func (ss *sqlSet) unavailableDiscreteValues() []string {
	var unavailableDiscreteValues []string
	for _, f := range ss.features {
		df, ok := f.(*botanic.DiscreteFeature)
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

func (ss *sqlSet) init() error {
	var err error
	ss.discreteValues, err = ss.db.ListDiscreteValues()
	if err != nil {
		return err
	}
	ss.inverseDiscreteValues = make(map[string]int)
	for k, v := range ss.discreteValues {
		ss.inverseDiscreteValues[v] = k
	}
	return nil
}

func (ss *sqlSet) newRawSample(s botanic.Sample) (map[string]interface{}, error) {
	rs := make(map[string]interface{})
	for _, f := range ss.features {
		v := s.ValueFor(f)
		if v != nil {
			_, ok := f.(*botanic.DiscreteFeature)
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

func (ss *sqlSet) initFeatureColumns() error {
	ss.columnFeatures = make(map[string]botanic.Feature)
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
		if _, ok := f.(*botanic.DiscreteFeature); ok {
			ss.dfColumns = append(ss.dfColumns, ss.featureNamesColumns[f.Name()])
		} else {
			ss.cfColumns = append(ss.cfColumns, ss.featureNamesColumns[f.Name()])
		}
	}
	return nil
}
