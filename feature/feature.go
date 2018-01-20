package feature

import "fmt"

/*
Feature represents a property that can be observed
*/
type Feature interface {
	Name() string
	Valid(interface{}) (bool, error)
}

/*
DiscreteFeature represents a property that can be observed and that can only
take a value among a finite set.
*/
type DiscreteFeature struct {
	name            string
	availableValues []string
}

/*
ContinuousFeature represents a property that can be observed and that can take
a numeric value
*/
type ContinuousFeature struct {
	name string
}

/*
NewDiscreteFeature takes a name string and a slice of available value strings
and returns a discrete feature with the given names and available values.
*/
func NewDiscreteFeature(name string, availableValues []string) *DiscreteFeature {
	return &DiscreteFeature{name, availableValues}
}

/*
NewContinuousFeature takes a name string and returns a continuous feature with
the given name.
*/
func NewContinuousFeature(name string) *ContinuousFeature {
	return &ContinuousFeature{name}
}

/*
Name returns a string with the name of the feature
*/
func (df *DiscreteFeature) Name() string {
	return df.name
}

/*
Valid receives an interface value and returns a boolean and an error. When the
value parameter is included in the available values fo the feature, the method
returns true and nil. Otherwise it returns false and an error describing the
reason.
*/
func (df *DiscreteFeature) Valid(value interface{}) (bool, error) {
	if value == nil {
		return true, nil
	}
	vs, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("discrete feature %s expects string value, got %T value", df.Name(), value)
	}
	for _, av := range df.availableValues {
		if av == vs {
			return true, nil
		}
	}
	return false, fmt.Errorf("discrete feature %s got unknown value %s", df.Name(), vs)
}

/*
AvailableValues returns a string slice with the values available for the feature
*/
func (df *DiscreteFeature) AvailableValues() []string {
	return df.availableValues
}

func (df *DiscreteFeature) String() string {
	return df.name
}

/*
Name returns a string with the name of the feature
*/
func (cf *ContinuousFeature) Name() string {
	return cf.name
}

/*
Valid receives an interface value and returns a boolean and an error. When the
value parameter is a float64 it returns true and nil, otherwise it returns
false and an error describing the reason.
*/
func (cf *ContinuousFeature) Valid(value interface{}) (bool, error) {
	if value == nil {
		return true, nil
	}
	_, ok := value.(float64)
	if !ok {
		return false, fmt.Errorf("continuous feature %s expects float64 value, got %T value", cf.Name(), value)
	}
	return true, nil
}

func (cf *ContinuousFeature) String() string {
	return cf.name
}
