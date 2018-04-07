package json

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pbanos/botanic/tree"

	"github.com/pbanos/botanic/feature"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/queue"
)

/*
TaskEncodeDecoder is an interface for objects
that allow encoding tasks as strings and decoding
them back from strinsts to tasks. It is used to
serialize tasks into a representation to store on.
redis
*/
type TaskEncodeDecoder interface {

	//Encode receives a *queue.Task
	// and returns a slice of bytes with the task encoded or an
	//error if the encoding could not be performed for
	//some reason. Its counterpart is TaskDecoder.
	Encode(context.Context, *queue.Task) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a *queue.Task decoded from the slice of bytes
	//or an error if the decoding could not be performed
	//for some reason.
	Decode(context.Context, []byte) (*queue.Task, error)
}

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

type jsonEncodeDecoder struct {
	features []feature.Feature
	ded      DatasetEncodeDecoder
	ns       tree.NodeStore
}

type jsonTask struct {
	NodeID                string          `json:"id"`
	AvailableFeatureNames []string        `json:"fs"`
	Dataset               json.RawMessage `json:"ds"`
}

func New(features []feature.Feature, ded DatasetEncodeDecoder, ns tree.NodeStore) TaskEncodeDecoder {
	return &jsonEncodeDecoder{features, ded, ns}
}

func (jed *jsonEncodeDecoder) Encode(ctx context.Context, t *queue.Task) ([]byte, error) {
	jt := &jsonTask{NodeID: t.ID()}
	for _, f := range t.AvailableFeatures {
		jt.AvailableFeatureNames = append(jt.AvailableFeatureNames, f.Name())
	}
	denc, err := jed.ded.Encode(ctx, t.Dataset)
	if err != nil {
		return nil, fmt.Errorf("encoding task as json: %v", err)
	}
	jt.Dataset = denc
	return json.Marshal(jt)
}
func (jed *jsonEncodeDecoder) Decode(ctx context.Context, data []byte) (*queue.Task, error) {
	jt := &jsonTask{}
	err := json.Unmarshal(data, jt)
	if err != nil {
		return nil, fmt.Errorf("decoding task from json: %v", err)
	}
	t := &queue.Task{}
	t.Node, err = jed.ns.Get(ctx, jt.NodeID)
	if err != nil {
		return nil, fmt.Errorf("decoding json task: getting task node: %v", err)
	}
	if t.Node == nil {
		return nil, fmt.Errorf("decoding json task: could not get node %q from node store", jt.NodeID)
	}
	for _, afn := range jt.AvailableFeatureNames {
		var af feature.Feature
		for _, f := range jed.features {
			if f.Name() == afn {
				af = f
				break
			}
		}
		if af == nil {
			return nil, fmt.Errorf("decoding json task: unknown feature %q", afn)
		}
		t.AvailableFeatures = append(t.AvailableFeatures, af)
	}
	t.Dataset, err = jed.ded.Decode(ctx, jt.Dataset)
	if err != nil {
		return nil, fmt.Errorf("decoding json task: decoding task dataset: %v", err)
	}
	return t, nil
}
