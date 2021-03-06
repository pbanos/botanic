package sqlset

import (
	"fmt"
	"math"

	"github.com/pbanos/botanic/feature"
)

/*
FeatureCriterion are used to represent
feature.Criterion on SQL DB-backed
sets, they should be easily translatable
to a condition on an SQL SELECT statement's
WHERE clause on a samples table.
*/
type FeatureCriterion struct {
	/*
		FeatureColumn is the column name for the feature.Feature
		the criterion is applying the restriction to.
	*/
	FeatureColumn string
	/*
		DiscreteFeature defines whether the feature criterion applies
		to a discrete feature
	*/
	DiscreteFeature bool
	/*
		Operator is a string representing the
		comparison against the value in the criterion
		that is applied to samples. It must be one of
		the following: "=", "<", ">", "<=" or ">=".
		The semantics are the result from reading
		the criterion as Feature Operator Value
	*/
	Operator string
	/*
		Value is the value against which a comparison
		is applied to samples. It should be either an
		integer for discrete features or a float64 for
		continuous features.
	*/
	Value interface{}
}

/*
ColumnNameFunc is a function that takes the name of a
feature and returns column name for it or an error if
the name could not be transformed.
*/
type ColumnNameFunc func(string) (string, error)

/*
NewFeatureCriteria takes a feature.Criterion, a ColumnNameFunc and a map of
string to int containing a dictionary for converting discrete string values into
their integer representations and returns a slice of FeatureCriterion equivalent
to the given feature.Criterion or an error.

An error will be returned the ColumnNameFunc cannot provide a name for the
feature of the feature criterion, or if the given feature.Criterion is a
feature.DiscreteCriterion and its value has no representation defined
on the given dictionary.

For a feature.Criterion that is no feature.DiscreteCriterion nor
feature.ContinuousCriterion it returns an empty slice and no error. In
other words, it is interpreted as an undefined feature criterion, which imposes
no conditions on samples.
*/
func NewFeatureCriteria(fc feature.Criterion, cnf ColumnNameFunc, dictionary map[string]int) ([]*FeatureCriterion, error) {
	columnName, err := cnf(fc.Feature().Name())
	if err != nil {
		return nil, fmt.Errorf("cannot obtain column name for feature '%s': %v", fc.Feature().Name(), err)
	}
	result := []*FeatureCriterion{}
	switch fc := fc.(type) {
	case feature.ContinuousCriterion:
		a, b := fc.Interval()
		if !math.IsInf(a, 0) {
			result = append(result, &FeatureCriterion{columnName, false, ">=", a})
		}
		if !math.IsInf(b, 0) {
			result = append(result, &FeatureCriterion{columnName, false, "<", b})
		}
	case feature.DiscreteCriterion:
		dvr, ok := dictionary[fc.Value()]
		if !ok {
			return nil, fmt.Errorf("non representable discrete value '%s' in feature criterion", fc.Value())
		}
		result = append(result, &FeatureCriterion{columnName, true, "=", dvr})
	}
	return result, nil
}
