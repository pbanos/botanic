package botanic

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

/*
FeatureCriterion represents a constraint on a feature

Its SatisfiedBy method takes a sample and returns a boolean indicating if
the given value satisfies the feature criterion.

Its Feature method returns the feature on which the criterion is applied.
*/
type FeatureCriterion interface {
	Feature() Feature
	SatisfiedBy(sample Sample) (bool, error)
}

/*
ContinuousFeatureCriterion represents a constraint on a continuous feature, a
range that delimits which values it may take. The interval can be open on one end,
thus representing -Infinity or +Infinity

Its Interval method returns the start and end of the interval to which the
feature is constrained as a pair of float64 values.
*/
type ContinuousFeatureCriterion interface {
	FeatureCriterion
	Interval() (float64, float64)
}

/*
DiscreteFeatureCriterion represents a constraint on a discrete feature, a
value it may take.

Its Value method returns the value to which the feature is constrained as
a string.
*/
type DiscreteFeatureCriterion interface {
	FeatureCriterion
	Value() string
}

type continuousFeatureCriterion struct {
	feature *ContinuousFeature
	a, b    float64
}

type discreteFeatureCriterion struct {
	feature *DiscreteFeature
	value   string
}

type undefinedFeatureCriterion struct {
	feature Feature
}

type jsonFeatureCriterion struct {
	Type    string `json:"type"`
	Feature string `json:"feature"`
	Value   string `json:"value,omitempty"`
	A       string `json:"a,omitempty"`
	B       string `json:"b,omitempty"`
}

/*
NewContinuousFeatureCriterion takes a ContinuousFeature feature and a pair of
float64 values indicating the start and the end of an interval and return a
ContinuousFeatureCriterion with the feature and interval. The interval can be
open on any end by providing -Inf and/or +Inf.
*/
func NewContinuousFeatureCriterion(feature *ContinuousFeature, a float64, b float64) ContinuousFeatureCriterion {
	return &continuousFeatureCriterion{feature, a, b}
}

/*
NewDiscreteFeatureCriterion takes a DiscreteFeature feature and a pair of
float64 values indicating the start and the end of an interval and return a
DiscreteFeatureCriterion with the feature and interval. The interval can be
open on any end by providing -Inf and/or +Inf.
*/
func NewDiscreteFeatureCriterion(feature *DiscreteFeature, value string) DiscreteFeatureCriterion {
	return &discreteFeatureCriterion{feature, value}
}

/*
NewUndefinedFeatureCriterion takes a Feature and returns a FeatureCriterion that
is always satisfied.
*/
func NewUndefinedFeatureCriterion(f Feature) FeatureCriterion {
	return &undefinedFeatureCriterion{f}
}

/*
Feature returns the feature to which the constraint applies.
*/
func (cfc *continuousFeatureCriterion) Feature() Feature {
	return cfc.feature
}

/*
SatisfiedBy receives a sample as parameter and returns a boolean indicating if the
sample satisfies the criterion. Specifically, it returns false if the sample does
not define a value for the feature, true if the value, being a float64, is in the
range defined by the criterion; and false otherwise.
*/
func (cfc *continuousFeatureCriterion) SatisfiedBy(sample Sample) (bool, error) {
	val, err := sample.ValueFor(cfc.feature)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	floatVal, ok := val.(float64)
	if !ok {
		return false, nil
	}
	return (math.IsInf(cfc.a, 0) || cfc.a <= floatVal) && (math.IsInf(cfc.b, 0) || floatVal < cfc.b), nil
}

func (cfc *continuousFeatureCriterion) Interval() (float64, float64) {
	return cfc.a, cfc.b
}

func (cfc *continuousFeatureCriterion) String() string {
	if math.IsInf(cfc.a, 0) {
		return fmt.Sprintf("%s < %f", cfc.feature.Name(), cfc.b)
	}
	if math.IsInf(cfc.b, 0) {
		return fmt.Sprintf("%f <= %s", cfc.a, cfc.feature.Name())
	}
	return fmt.Sprintf("%f <= %s < %f", cfc.a, cfc.feature.Name(), cfc.b)
}

func (cfc *continuousFeatureCriterion) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonFeatureCriterion{
		Type:    "continuous",
		Feature: cfc.feature.Name(),
		A:       fmt.Sprintf("%f", cfc.a),
		B:       fmt.Sprintf("%f", cfc.b),
	})
}

/*
Feature returns the feature to which the constraint applies.
*/
func (dfc *discreteFeatureCriterion) Feature() Feature {
	return dfc.feature
}

/*
SatisfiedBy receives a sample as parameter and returns a boolean indicating if the
sample satisfies the criterion. Specifically, it returns false if the sample does
not define a value for the feature, true if the value, being a string, equals the
value on the criterion; and false otherwise.
*/
func (dfc *discreteFeatureCriterion) SatisfiedBy(sample Sample) (bool, error) {
	val, err := sample.ValueFor(dfc.feature)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	stringVal, ok := val.(string)
	if !ok {
		return false, nil
	}
	return dfc.value == stringVal, nil
}

func (dfc *discreteFeatureCriterion) Value() string {
	return dfc.value
}

func (dfc *discreteFeatureCriterion) String() string {
	return fmt.Sprintf("%s is %s", dfc.feature.Name(), dfc.value)
}

func (dfc *discreteFeatureCriterion) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonFeatureCriterion{
		Type:    "discrete",
		Feature: dfc.feature.Name(),
		Value:   dfc.value,
	})
}

func (u *undefinedFeatureCriterion) Feature() Feature {
	return u.feature
}

func (u *undefinedFeatureCriterion) SatisfiedBy(sample Sample) (bool, error) {
	return true, nil
}

func (u *undefinedFeatureCriterion) String() string {
	return fmt.Sprintf("%s not defined", u.feature.Name())
}

func (u *undefinedFeatureCriterion) MarshalJSON() ([]byte, error) {
	return json.Marshal(&jsonFeatureCriterion{
		Type:    "undefined",
		Feature: u.feature.Name(),
	})
}

func (jfc *jsonFeatureCriterion) FeatureCriterion() (FeatureCriterion, error) {
	switch jfc.Type {
	case "continuous":
		return jfc.toContinuousFeatureCriterion()
	case "discrete":
		return jfc.toDiscreteFeatureCriterion()
	case "undefined":
		return jfc.toUndefinedFeatureCriterion()
	}
	return nil, fmt.Errorf("unknown feature criterion type '%s'", jfc.Type)
}

func (jfc *jsonFeatureCriterion) toUndefinedFeatureCriterion() (FeatureCriterion, error) {
	f := &UndefinedFeature{jfc.Feature}
	return &undefinedFeatureCriterion{f}, nil
}

func (jfc *jsonFeatureCriterion) toDiscreteFeatureCriterion() (FeatureCriterion, error) {
	f := NewDiscreteFeature(jfc.Feature, []string{jfc.Value})
	return &discreteFeatureCriterion{f, jfc.Value}, nil
}

func (jfc *jsonFeatureCriterion) toContinuousFeatureCriterion() (FeatureCriterion, error) {
	fc := &continuousFeatureCriterion{}
	fc.feature = &ContinuousFeature{jfc.Feature}
	var err error
	if jfc.A == "-Inf" {
		fc.a = math.Inf(-1)
	} else {
		fc.a, err = strconv.ParseFloat(jfc.A, 64)
		if err != nil {
			return nil, err
		}
	}
	if jfc.B == "+Inf" {
		fc.b = math.Inf(+1)
	} else {
		fc.b, err = strconv.ParseFloat(jfc.B, 64)
		if err != nil {
			return nil, err
		}
	}
	return fc, nil
}
