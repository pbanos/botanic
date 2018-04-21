package feature

import (
	"context"
	"fmt"
	"math"
)

/*
Criterion represents a constraint on a feature

Its SatisfiedBy method takes a sample and returns a boolean indicating if
the given value satisfies the feature criterion.

Its Feature method returns the feature on which the criterion is applied.
*/
type Criterion interface {
	Feature() Feature
	SatisfiedBy(ctx context.Context, sample Sample) (bool, error)
}

/*
Sample is an interface for something that can satisfy a Criterion.

Its ValueFor method returns the value corresponding to the feature
passed as parameter.
*/
type Sample interface {
	ValueFor(context.Context, Feature) (interface{}, error)
}

/*
ContinuousCriterion represents a constraint on a continuous feature, a
range that delimits which values it may take. The interval can be open on one end,
thus representing -Infinity or +Infinity

Its Interval method returns the start and end of the interval to which the
feature is constrained as a pair of float64 values.
*/
type ContinuousCriterion interface {
	Criterion
	Interval() (float64, float64)
}

/*
DiscreteCriterion represents a constraint on a discrete feature, a
value it may take.

Its Value method returns the value to which the feature is constrained as
a string.
*/
type DiscreteCriterion interface {
	Criterion
	Value() string
}

/*
UndefinedCriterion represents the lack of constraint on a specific feature.
*/
type UndefinedCriterion interface {
	Criterion
	IsUndefinedCriterion() bool
}

type continuousCriterion struct {
	feature *ContinuousFeature
	a, b    float64
}

type discreteCriterion struct {
	feature *DiscreteFeature
	value   string
}

type undefinedCriterion struct {
	feature Feature
}

/*
NewContinuousCriterion takes a ContinuousFeature feature and a pair of
float64 values indicating the start and the end of an interval and return a
ContinuousCriterion with the feature and interval. The interval can be
open on any end by providing -Inf and/or +Inf.
*/
func NewContinuousCriterion(feature *ContinuousFeature, a float64, b float64) ContinuousCriterion {
	return &continuousCriterion{feature, a, b}
}

/*
NewDiscreteCriterion takes a DiscreteFeature feature and a pair of
float64 values indicating the start and the end of an interval and return a
DiscreteCriterion with the feature and interval. The interval can be
open on any end by providing -Inf and/or +Inf.
*/
func NewDiscreteCriterion(feature *DiscreteFeature, value string) DiscreteCriterion {
	return &discreteCriterion{feature, value}
}

/*
NewUndefinedCriterion takes a Feature and returns a Criterion that
is always satisfied.
*/
func NewUndefinedCriterion(f Feature) UndefinedCriterion {
	return &undefinedCriterion{f}
}

/*
Feature returns the feature to which the constraint applies.
*/
func (cfc *continuousCriterion) Feature() Feature {
	return cfc.feature
}

/*
SatisfiedBy receives a sample as parameter and returns a boolean indicating if the
sample satisfies the criterion. Specifically, it returns false if the sample does
not define a value for the feature, true if the value, being a float64, is in the
range defined by the criterion; and false otherwise.
*/
func (cfc *continuousCriterion) SatisfiedBy(ctx context.Context, sample Sample) (bool, error) {
	val, err := sample.ValueFor(ctx, cfc.feature)
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

func (cfc *continuousCriterion) Interval() (float64, float64) {
	return cfc.a, cfc.b
}

func (cfc *continuousCriterion) String() string {
	if math.IsInf(cfc.a, 0) {
		return fmt.Sprintf("%s < %f", cfc.feature.Name(), cfc.b)
	}
	if math.IsInf(cfc.b, 0) {
		return fmt.Sprintf("%f <= %s", cfc.a, cfc.feature.Name())
	}
	return fmt.Sprintf("%f <= %s < %f", cfc.a, cfc.feature.Name(), cfc.b)
}

/*
Feature returns the feature to which the constraint applies.
*/
func (dfc *discreteCriterion) Feature() Feature {
	return dfc.feature
}

/*
SatisfiedBy receives a sample as parameter and returns a boolean indicating if the
sample satisfies the criterion. Specifically, it returns false if the sample does
not define a value for the feature, true if the value, being a string, equals the
value on the criterion; and false otherwise.
*/
func (dfc *discreteCriterion) SatisfiedBy(ctx context.Context, sample Sample) (bool, error) {
	val, err := sample.ValueFor(ctx, dfc.feature)
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

func (dfc *discreteCriterion) Value() string {
	return dfc.value
}

func (dfc *discreteCriterion) String() string {
	return fmt.Sprintf("%s is %s", dfc.feature.Name(), dfc.value)
}

func (u *undefinedCriterion) Feature() Feature {
	return u.feature
}

func (u *undefinedCriterion) SatisfiedBy(context.Context, Sample) (bool, error) {
	return true, nil
}

func (u *undefinedCriterion) IsUndefinedCriterion() bool {
	return true
}

func (u *undefinedCriterion) String() string {
	return fmt.Sprintf("%s not defined", u.feature.Name())
}
