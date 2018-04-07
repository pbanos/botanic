package json

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

/*
DatasetEncodeDecoder is an interface for objects
that allow encoding datasets into slices of
bytes and decoding them back to datasets.
*/
type DatasetEncodeDecoder interface {

	//Encode receives a *dataset.Dataset
	// and returns a slice of bytes with the dataset
	//encoded or an error if the encoding could not
	//be performed for some reason.
	Encode(context.Context, dataset.Dataset) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a *dataset.Dataset decoded from the
	//slice of bytes or an error if the decoding
	//could not be performed for some reason.
	Decode(context.Context, []byte) (dataset.Dataset, error)
}

/*
CriteriaEncodeDecoder is an interface for objects
that allow encoding criteria into slices of
bytes and decoding them back to criteria.
*/
type CriteriaEncodeDecoder interface {

	//Encode receives a feature.Criterion
	// and returns a slice of bytes with the dataset
	//encoded or an error if the encoding could not
	//be performed for some reason.
	Encode(feature.Criterion) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a feature.Criterion decoded from the
	//slice of bytes or an error if the decoding
	//could not be performed for some reason.
	Decode([]byte) (feature.Criterion, error)
}

type jsonEncodeDecoder struct {
	ced            CriteriaEncodeDecoder
	rootDataset    dataset.Dataset
	rootDatasetURI string
}

type jsonDataset struct {
	URI      string            `json:"uri"`
	Criteria []json.RawMessage `json:"criteria"`
}

/*
New takes a dataset, an URI for it and a CriteriaEncodeDecoder and returns
a DatasetEncodeDecoder that encodes/decodes datasets as JSON objects, representing them
as the given URI with the criteria encoded using the given CriteriaEncodeDecoder
*/
func New(rootDataset dataset.Dataset, rootDatasetURI string, ced CriteriaEncodeDecoder) DatasetEncodeDecoder {
	return &jsonEncodeDecoder{
		ced:            ced,
		rootDataset:    rootDataset,
		rootDatasetURI: rootDatasetURI,
	}
}

func (jed *jsonEncodeDecoder) Encode(ctx context.Context, ds dataset.Dataset) ([]byte, error) {
	cs, err := ds.Criteria(ctx)
	if err != nil {
		return nil, fmt.Errorf("obtaining dataset criteria: %v", err)
	}
	criteria := make([]json.RawMessage, 0, len(cs))
	for _, c := range cs {
		ec, err := jed.ced.Encode(c)
		if err != nil {
			return nil, fmt.Errorf("encoding criterion #%q: %v", c, err)
		}
		criteria = append(criteria, ec)
	}
	res := &jsonDataset{
		URI:      jed.rootDatasetURI,
		Criteria: criteria,
	}
	return json.Marshal(res)
}
func (jed *jsonEncodeDecoder) Decode(ctx context.Context, data []byte) (dataset.Dataset, error) {
	jds := &jsonDataset{}
	err := json.Unmarshal(data, jds)
	if err != nil {
		return nil, err
	}
	if jds.URI != jed.rootDatasetURI {
		return nil, fmt.Errorf("decoded dataset does not have the right root dataset URI: found %q, expected %q", jds.URI, jed.rootDatasetURI)
	}
	ds := jed.rootDataset
	for _, ec := range jds.Criteria {
		c, err := jed.ced.Decode(ec)
		if err != nil {
			return nil, fmt.Errorf("decoding dataset criteria: %v", err)
		}
		ds, err = ds.SubsetWith(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("decoding dataset: applying criteria %v: %v", c, err)
		}
	}
	return ds, nil
}
