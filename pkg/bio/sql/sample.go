package sql

import (
	"fmt"

	"github.com/pbanos/botanic/pkg/botanic"
)

/*
Sample is an implementation of botanic.Sample
optimized to represent samples belonging to Set.
*/
type Sample struct {
	/*
		Values is a map of string columns names to interface{}.
		Specifically, the value must be
		* nil for an undefined value for any feature the column
		  is representing or
		* an int for the value of a discrete feature the column
		  is representing or
		* a float64 for the value of a continuous feature the
		  column is representing
	*/
	Values map[string]interface{}
	/*
		DiscreteFeatureValues is a map of int to strings
		that holds the relation of int representations on
		the Sample's Values map to their string
		representations
	*/
	DiscreteFeatureValues map[int]string
	/*
		FeatureNamesColumns is a map that translates the name
		of a feature to the column representing it on the database.
		This column is also the string value that acts as key for
		the feature value on the Sample's Value map.
	*/
	FeatureNamesColumns map[string]string
}

/*
ValueFor takes a feature and returns the value for the feature
according to the sample or nil if is undefined. For continuous
feature the value is the one available on its Values map for
the name of the column corresponding to the feature's name,
whereas for discrete features this value is used as key on the
DiscreteFeaturesValue dictionary to obtain the string
representation for it.
*/
func (s *Sample) ValueFor(f botanic.Feature) (interface{}, error) {
	c, ok := s.FeatureNamesColumns[f.Name()]
	if !ok {
		return nil, nil
	}
	v, ok := s.Values[c]
	if !ok {
		return nil, nil
	}
	_, ok = f.(*botanic.DiscreteFeature)
	if ok {
		iv, ok := v.(int)
		if !ok {
			return nil, fmt.Errorf("expected sql representation for the value of %s to be an int, got %T", f.Name(), v)
		}
		v = interface{}(s.DiscreteFeatureValues[iv])
	}
	return v, nil
}
