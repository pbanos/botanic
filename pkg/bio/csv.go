package bio

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pbanos/botanic/pkg/botanic"
)

/*
SetGenerator is a function that takes a slice of samples
and generates a set with them.
*/
type SetGenerator func([]botanic.Sample) botanic.Set

/*
ReadCSVSet takes an io.Reader for a CSV stream, a slice of features and a
SetGenerator and returns botanic.Set built with the SetGenerator and the
samples parsed from the reader or an error.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadCSVSet(reader io.Reader, features []botanic.Feature, sg SetGenerator) (botanic.Set, error) {
	featuresByName := featureSliceToMap(features)
	r := csv.NewReader(reader)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("reading header: %v", err)
	}
	features, err = parseFeaturesFromCSVHeader(header, featuresByName)
	if err != nil {
		return nil, err
	}
	samples := []botanic.Sample{}
	for l := 2; ; l++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading body: %v", err)
		}
		sample, err := parseSampleFromCSVRow(row, features)
		if err != nil {
			return nil, fmt.Errorf("parsing line %d from %v: %v", l, reader, err)
		}
		samples = append(samples, sample)
	}
	return sg(samples), nil
}

/*
ReadCSVSetFromFilePath takes a filepath string, a slice of features and a SetGenerator,
opens the file to which the filepath points to and uses ReadCSVSet to return a
botanic.Set or an error read from it. It will return an error if the given filepath
cannot be opened for reading.
*/
func ReadCSVSetFromFilePath(filepath string, features []botanic.Feature, sg SetGenerator) (botanic.Set, error) {
	var f *os.File
	var err error
	if filepath == "" {
		f = os.Stdin
	} else {
		f, err = os.Open(filepath)
		if err != nil {
			return nil, fmt.Errorf("reading training set: %v", err)
		}
	}
	defer f.Close()
	set, err := ReadCSVSet(f, features, sg)
	if err != nil {
		err = fmt.Errorf("parsing CSV file %s: %v", filepath, err)
	}
	return set, err
}

func parseFeaturesFromCSVHeader(header []string, features map[string]botanic.Feature) ([]botanic.Feature, error) {
	featureOrder := []botanic.Feature{}
	for i, name := range header {
		f, ok := features[name]
		if ok {
			featureOrder = append(featureOrder, f)
		} else {
			if i != len(header)-1 {
				return nil, fmt.Errorf("parsing header: reference to unknown feature %s", name)
			}
		}
	}
	return featureOrder, nil
}

func parseSampleFromCSVRow(row []string, featureOrder []botanic.Feature) (botanic.Sample, error) {
	featureValues := make(map[string]interface{})
	for i, feature := range featureOrder {
		v := row[i]
		var value interface{}
		var err error
		var ok bool
		if v != "?" {
			if _, ok = feature.(*botanic.ContinuousFeature); ok {
				value, err = strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("converting %s to float64: %v", v, err)
				}
			} else {
				value = v
			}
		}
		if ok, err = feature.Valid(value); !ok {
			return nil, fmt.Errorf("invalid value %v of type %T for feature %s: %v", value, value, feature.Name(), err)
		}
		featureValues[feature.Name()] = value
	}
	return botanic.NewSample(featureValues), nil
}

func featureSliceToMap(features []botanic.Feature) map[string]botanic.Feature {
	result := make(map[string]botanic.Feature)
	for _, f := range features {
		result[f.Name()] = f
	}
	return result
}
