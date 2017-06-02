package bio

import (
	"bufio"
	"io"
	"os"
	"strconv"

	"github.com/pbanos/botanic/pkg/botanic"
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
	features              []botanic.Feature
}

/*
FeatureValueRequester represents a way to ask
for feature values and reject the given values.
*/
type FeatureValueRequester interface {
	RequestValueFor(botanic.Feature) error
	RejectValueFor(botanic.Feature, interface{}) error
}

/*
NewReadSample takes an io.Reader, a slice of features, a
FeatureValueRequester and an undefinedValue coding string
and returns a Sample.

The returned Sample ValueFor method reads feature values first
requesting them with the given FeatureValueRequester and
then parsing the values from the reader.

The parsing expects each value to be presented ending with the
'\n' character, that is in new lines. Also, the undefinedValue
string followed by the '\n' character will be interpreted as an
undefined value.

For a botanic.ContinuousFeature, lines will be read from the
reader until a line containing a valid float64 number is found.

For a botanic.DiscreteFeature, lines will be read from the
reader until a line with a valid value for the feature is found.

For both kind of botanic.Feature, non accepted values will be
rejected with the FeatureValueRequester's RejectValueFor method.

Attempting to obtain a value for Feature not in the given
features slice, or for another type of feature will return nil.
*/
func NewReadSample(r io.Reader, features []botanic.Feature, featureValueRequester FeatureValueRequester, undefinedValue string) botanic.Sample {
	scanner := bufio.NewScanner(os.Stdin)
	return &readSample{make(map[string]interface{}), undefinedValue, scanner, featureValueRequester, features}
}

func (rs *readSample) ValueFor(f botanic.Feature) interface{} {
	value, ok := rs.obtainedValues[f.Name()]
	if ok {
		return value
	}
	var featureWithInfo botanic.Feature
	for _, feature := range rs.features {
		if f.Name() == feature.Name() {
			featureWithInfo = feature
		}
	}
	if featureWithInfo == nil {
		return nil
	}
	err := rs.featureValueRequester.RequestValueFor(featureWithInfo)
	if err != nil {
		return nil
	}
	switch featureWithInfo := featureWithInfo.(type) {
	case *botanic.ContinuousFeature:
		return rs.readContinuousFeature(featureWithInfo)
	case *botanic.DiscreteFeature:
		return rs.readDiscreteFeature(featureWithInfo)
	}
	return nil
}

func (rs *readSample) readContinuousFeature(f botanic.Feature) interface{} {
	for rs.scanner.Scan() {
		line := rs.scanner.Text()
		if line == rs.undefinedValue {
			rs.obtainedValues[f.Name()] = nil
			break
		}
		value, err := strconv.ParseFloat(line, 64)
		if err != nil {
			rs.obtainedValues[f.Name()] = value
			return value
		}
		err = rs.featureValueRequester.RejectValueFor(f, line)
		if err != nil {
			break
		}
	}
	return nil
}

func (rs *readSample) readDiscreteFeature(df *botanic.DiscreteFeature) interface{} {
	for rs.scanner.Scan() {
		line := rs.scanner.Text()
		if line == rs.undefinedValue {
			rs.obtainedValues[df.Name()] = nil
			break
		}
		for _, v := range df.AvailableValues() {
			if v == line {
				rs.obtainedValues[df.Name()] = v
				return v
			}
		}
		err := rs.featureValueRequester.RejectValueFor(df, line)
		if err != nil {
			break
		}
	}
	return nil
}
