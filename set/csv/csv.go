package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/set"
)

/*
Writer is an interface for a set to which samples
can be written to.
*/
type Writer interface {
	// Write will attempt to write the given number
	// of samples and will return the actually written
	// number of samples and an error (if not all samples
	// could be written)
	Write(context.Context, []set.Sample) (int, error)
	// Count returns the total number of samples written
	// to the writer
	Count() int
	// Flush ensures any pending written operations finish
	// before returning. It returns an error if that cannot
	// be ensured.
	Flush() error
}

/*
SetGenerator is a function that takes a slice of samples
and generates a set with them.
*/
type SetGenerator func([]set.Sample) set.Set

type csvWriter struct {
	count    int
	features []feature.Feature
	w        *csv.Writer
}

/*
ReadSet takes an io.Reader for a CSV stream, a slice of features and a
SetGenerator and returns set.Set built with the SetGenerator and the
samples parsed from the reader or an error.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadSet(reader io.Reader, features []feature.Feature, sg SetGenerator) (set.Set, error) {
	samples := []set.Sample{}
	err := ReadSetBySample(reader, features, func(_ int, s set.Sample) (bool, error) {
		samples = append(samples, s)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return sg(samples), nil
}

/*
ReadSetBySample takes an io.Reader for a CSV stream, a slice of features and a
lambda function on an integer and a set.Sample that returns a boolean value.
It parses the samples from the reader and for each it calls the lambda function
with the sample and its index as parameters. If the lambda function returns true,
it will continue processing the next sample, otherwise it will stop. An error is
returned if something goes wrong when reading the file or parsing a sample.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadSetBySample(reader io.Reader, features []feature.Feature, lambda func(int, set.Sample) (bool, error)) error {
	featuresByName := featureSliceToMap(features)
	r := csv.NewReader(reader)
	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("reading header: %v", err)
	}
	features, err = parseFeaturesFromCSVHeader(header, featuresByName)
	if err != nil {
		return err
	}
	for l := 2; ; l++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading body: %v", err)
		}
		sample, err := parseSampleFromCSVRow(row, features)
		if err != nil {
			return fmt.Errorf("parsing line %d from %v: %v", l, reader, err)
		}
		ok, err := lambda(l-2, sample)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	return nil
}

/*
ReadSetFromFilePath takes a filepath string, a slice of features and a SetGenerator,
opens the file to which the filepath points to and uses ReadSet to return a
set.Set or an error read from it. It will return an error if the given filepath
cannot be opened for reading.
*/
func ReadSetFromFilePath(filepath string, features []feature.Feature, sg SetGenerator) (set.Set, error) {
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
	set, err := ReadSet(f, features, sg)
	if err != nil {
		err = fmt.Errorf("parsing CSV file %s: %v", filepath, err)
	}
	return set, err
}

/*
ReadSetBySampleFromFilePath takes an filepath string for a CSV stream, a
slice of features and a lambda function on an integer and a set.Sample
that returns a boolean value. It opens the file for reading (if the filapath
is "" os.Stdin is used instead), parses the samples from the reader and for
each it calls the lambda function with the sample and its index as parameters.
If the lambda function returns true, it will continue processing the next
sample, otherwise it will stop. An error is returned if something goes wrong
when reading the file or parsing a sample.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadSetBySampleFromFilePath(filepath string, features []feature.Feature, lambda func(int, set.Sample) (bool, error)) error {
	var f *os.File
	var err error
	if filepath == "" {
		f = os.Stdin
	} else {
		f, err = os.Open(filepath)
		if err != nil {
			return fmt.Errorf("reading training set: %v", err)
		}
	}
	defer f.Close()
	err = ReadSetBySample(f, features, lambda)
	if err != nil {
		return err
	}
	return nil
}

/*
NewWriter takes an io.Writer and a slice of feature.Features and
returns a Writer that will write any samples on the io.Writer.
*/
func NewWriter(writer io.Writer, features []feature.Feature) (Writer, error) {
	w := csv.NewWriter(writer)
	record := make([]string, len(features))
	for i, f := range features {
		record[i] = f.Name()
	}
	err := w.Write(record)
	if err != nil {
		return nil, fmt.Errorf("writing CSV header: %v", err)
	}
	return &csvWriter{features: features, w: w}, nil
}

/*
WriteCSVSet takes a writer, a set.Set and a slice of features and
dumps to the writer the set in CSV format, specifying only the features
in the given slice for the samples. It returns an error if something
went wrong when wrting to the writer, or codifying the samples.
*/
func WriteCSVSet(ctx context.Context, writer io.Writer, s set.Set, features []feature.Feature) error {
	cw, err := NewWriter(writer, features)
	if err != nil {
		return err
	}
	samples, err := s.Samples(ctx)
	if err != nil {
		return err
	}
	_, err = cw.Write(ctx, samples)
	if err != nil {
		return err
	}
	return cw.Flush()
}

func parseFeaturesFromCSVHeader(header []string, features map[string]feature.Feature) ([]feature.Feature, error) {
	featureOrder := []feature.Feature{}
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

func parseSampleFromCSVRow(row []string, featureOrder []feature.Feature) (set.Sample, error) {
	featureValues := make(map[string]interface{})
	for i, f := range featureOrder {
		v := row[i]
		var value interface{}
		var err error
		var ok bool
		if v != "?" {
			if _, ok = f.(*feature.ContinuousFeature); ok {
				value, err = strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("converting %s to float64: %v", v, err)
				}
			} else {
				value = v
			}
		}
		if ok, err = f.Valid(value); !ok {
			return nil, fmt.Errorf("invalid value %v of type %T for feature %s: %v", value, value, f.Name(), err)
		}
		featureValues[f.Name()] = value
	}
	return set.NewSample(featureValues), nil
}

func (cw *csvWriter) Count() int {
	return cw.count
}

func (cw *csvWriter) Write(ctx context.Context, samples []set.Sample) (int, error) {
	n := 0
	var err error
	for ; n < len(samples); n++ {
		err = cw.WriteSample(samples[n])
		if err != nil {
			return n, err
		}
	}
	return len(samples), nil
}

func (cw *csvWriter) WriteSample(sample set.Sample) error {
	record := make([]string, len(cw.features))
	for j, f := range cw.features {
		v, err := sample.ValueFor(f)
		if err != nil {
			return err
		}
		if v == nil {
			record[j] = "?"
		} else {
			record[j] = fmt.Sprintf("%v", v)
		}
	}
	err := cw.w.Write(record)
	if err != nil {
		return fmt.Errorf("writing CSV row for sample %d: %v", cw.count+1, err)
	}
	cw.count++
	return nil
}

func (cw *csvWriter) Flush() error {
	cw.w.Flush()
	return cw.w.Error()
}

func featureSliceToMap(features []feature.Feature) map[string]feature.Feature {
	result := make(map[string]feature.Feature)
	for _, f := range features {
		result[f.Name()] = f
	}
	return result
}
