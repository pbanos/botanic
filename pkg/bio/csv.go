package bio

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/pbanos/botanic/pkg/botanic"
)

/*
 */
type CSVWriter interface {
	Write(context.Context, []botanic.Sample) (int, error)
	Count() int
	Flush() error
}

/*
SetGenerator is a function that takes a slice of samples
and generates a set with them.
*/
type SetGenerator func([]botanic.Sample) botanic.Set

type csvWriter struct {
	count    int
	features []botanic.Feature
	w        *csv.Writer
}

/*
ReadCSVSet takes an io.Reader for a CSV stream, a slice of features and a
SetGenerator and returns botanic.Set built with the SetGenerator and the
samples parsed from the reader or an error.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadCSVSet(reader io.Reader, features []botanic.Feature, sg SetGenerator) (botanic.Set, error) {
	samples := []botanic.Sample{}
	err := ReadCSVSetBySample(reader, features, func(_ int, s botanic.Sample) (bool, error) {
		samples = append(samples, s)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return sg(samples), nil
}

/*
ReadCSVSetBySample takes an io.Reader for a CSV stream, a slice of features and a
lambda function on an integer and a botanic.Sample that returns a boolean value.
It parses the samples from the reader and for each it calls the lambda function
with the sample and its index as parameters. If the lambda function returns true,
it will continue processing the next sample, otherwise it will stop. An error is
returned if something goes wrong when reading the file or parsing a sample.

The header or first row of the CSV content is expected to consist of the names
of the features in the given slice. The rest of the rows should consist of valid
values for the all features and/or the '?' string to indicate an undefined value.
*/
func ReadCSVSetBySample(reader io.Reader, features []botanic.Feature, lambda func(int, botanic.Sample) (bool, error)) error {
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

/*
ReadCSVSetBySampleFromFilePath takes an filepath string for a CSV stream, a
slice of features and a lambda function on an integer and a botanic.Sample
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
func ReadCSVSetBySampleFromFilePath(filepath string, features []botanic.Feature, lambda func(int, botanic.Sample) (bool, error)) error {
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
	err = ReadCSVSetBySample(f, features, lambda)
	if err != nil {
		return err
	}
	return nil
}

/*
NewCSVWriter takes an io.Writer and a slice of botanic.Features and
returns a CSVWriter that will write any samples on the io.Writer.
*/
func NewCSVWriter(writer io.Writer, features []botanic.Feature) (CSVWriter, error) {
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
WriteCSVSet takes a writer, a botanic.Set and a slice of features and
dumps to the writer the set in CSV format, specifying only the features
in the given slice for the samples. It returns an error if something
went wrong when wrting to the writer, or codifying the samples.
*/
func WriteCSVSet(ctx context.Context, writer io.Writer, s botanic.Set, features []botanic.Feature) error {
	cw, err := NewCSVWriter(writer, features)
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

func (cw *csvWriter) Count() int {
	return cw.count
}

func (cw *csvWriter) Write(ctx context.Context, samples []botanic.Sample) (int, error) {
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

func (cw *csvWriter) WriteSample(sample botanic.Sample) error {
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

func featureSliceToMap(features []botanic.Feature) map[string]botanic.Feature {
	result := make(map[string]botanic.Feature)
	for _, f := range features {
		result[f.Name()] = f
	}
	return result
}
