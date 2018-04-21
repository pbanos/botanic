/*
Package inputsample provides an implementation of dataset.Sample that is read
from an io.Reader.
*/
package inputsample

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

/*
ReadSample represents a sample whose feature values
are retrieved from a reader. A feature value will be
requested using a FeatureValueRequester before reading it.
*/
type readSample struct {
	obtainedValues        map[string]interface{}
	undefinedValue        string
	scanner               *bufio.Scanner
	featureValueRequester FeatureValueRequester
	features              []feature.Feature
}

/*
FeatureValueRequester represents a way to ask
for feature values and reject the given values.
*/
type FeatureValueRequester interface {
	RequestValueFor(feature.Feature) error
	RejectValueFor(feature.Feature, interface{}) error
}

/*
New takes an io.Reader, a slice of features, a
FeatureValueRequester and an undefinedValue coding string
and returns a Sample.

The returned Sample ValueFor method reads feature values first
requesting them with the given FeatureValueRequester and
then parsing the values from the reader.

The parsing expects each value to be presented ending with the
'\n' character, that is in new lines. Also, the undefinedValue
string followed by the '\n' character will be interpreted as an
undefined value.

For a feature.ContinuousFeature, lines will be read from the
reader until a line containing a valid float64 number is found.

For a feature.DiscreteFeature, lines will be read from the
reader until a line with a valid value for the feature is found.

For both kind of feature.Feature, non accepted values will be
rejected with the FeatureValueRequester's RejectValueFor method.

Attempting to obtain a value for Feature not in the given
features slice, or for another type of feature will return nil.
*/
func New(r io.Reader, features []feature.Feature, featureValueRequester FeatureValueRequester, undefinedValue string) dataset.Sample {
	scanner := bufio.NewScanner(os.Stdin)
	return &readSample{make(map[string]interface{}), undefinedValue, scanner, featureValueRequester, features}
}

func (rs *readSample) ValueFor(_ context.Context, f feature.Feature) (interface{}, error) {
	value, ok := rs.obtainedValues[f.Name()]
	if ok {
		return value, nil
	}
	var featureWithInfo feature.Feature
	for _, feature := range rs.features {
		if f.Name() == feature.Name() {
			featureWithInfo = feature
		}
	}
	if featureWithInfo == nil {
		return nil, fmt.Errorf("have no information about feature %s, do not know how to read its value", f.Name())
	}
	err := rs.featureValueRequester.RequestValueFor(featureWithInfo)
	if err != nil {
		return nil, err
	}
	switch featureWithInfo := featureWithInfo.(type) {
	case *feature.ContinuousFeature:
		return rs.readContinuousFeature(featureWithInfo)
	case *feature.DiscreteFeature:
		return rs.readDiscreteFeature(featureWithInfo)
	}
	return nil, fmt.Errorf("do not know how to read a value for features of type %T", featureWithInfo)
}

func (rs *readSample) readContinuousFeature(f feature.Feature) (interface{}, error) {
	var value float64
	var err error
	for rs.scanner.Scan() {
		line := rs.scanner.Text()
		if line == rs.undefinedValue {
			rs.obtainedValues[f.Name()] = nil
			return nil, nil
		}
		value, err = strconv.ParseFloat(line, 64)
		if err == nil {
			rs.obtainedValues[f.Name()] = value
			return value, nil
		}
		err = rs.featureValueRequester.RejectValueFor(f, line)
		if err != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	err = rs.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("EOF when requesting value")
}

func (rs *readSample) readDiscreteFeature(df *feature.DiscreteFeature) (interface{}, error) {
	var err error
	for rs.scanner.Scan() {
		line := rs.scanner.Text()
		if line == rs.undefinedValue {
			rs.obtainedValues[df.Name()] = nil
			return nil, nil
		}
		for _, v := range df.AvailableValues() {
			if v == line {
				rs.obtainedValues[df.Name()] = v
				return v, nil
			}
		}
		err = rs.featureValueRequester.RejectValueFor(df, line)
		if err != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	err = rs.scanner.Err()
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("EOF when requesting value")
}
