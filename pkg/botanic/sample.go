package botanic

import "fmt"

/*
Sample represents an item to process or from which to learn how to process them.

Its ValueFor method returns the value of the sample corresponding to the feature
passed as parameter.

Its Class method returns the class assigned for the sample
*/
type Sample interface {
	ValueFor(Feature) (interface{}, error)
}

type sample struct {
	featureValues map[string]interface{}
}

/*
NewSample takes a map of feature string names to values and a class and returns
a sample.
*/
func NewSample(featureValues map[string]interface{}) Sample {
	return &sample{featureValues}
}

func (s *sample) ValueFor(feature Feature) (interface{}, error) {
	return s.featureValues[feature.Name()], nil
}

func (s *sample) String() string {
	return fmt.Sprintf("[%v]", s.featureValues)
}
