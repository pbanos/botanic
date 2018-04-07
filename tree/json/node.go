package json

import (
	"encoding/json"
	"fmt"

	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/tree"
)

/*
NodeEncodeDecoder is an interface for objects
that allow encoding nodes into slices of
bytes and decoding them back to nodes.
*/
type NodeEncodeDecoder interface {

	//Encode receives a *tree.Node
	// and returns a slice of bytes with the dataset
	//encoded or an error if the encoding could not
	//be performed for some reason.
	Encode(*tree.Node) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a *tree.Node decoded from the
	//slice of bytes or an error if the decoding
	//could not be performed for some reason.
	Decode([]byte) (*tree.Node, error)
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

type nodeEncodeDecoder struct {
	CriteriaEncodeDecoder
	features []feature.Feature
}

type node struct {
	ID               string           `json:"id"`
	ParentID         string           `json:"pId,omitempty"`
	SubtreeIDs       []string         `json:"stIds,omitempty"`
	FeatureCriterion *json.RawMessage `json:"c,omitempty"`
	SubtreeFeature   string           `json:"f,omitempty"`
	Prediction       *json.RawMessage `json:"pred,omitempty"`
}

type jsonPrediction struct {
	Probabilities map[string]float64 `json:"probs,omitempty"`
	Weight        int                `json:"w,omitempty"`
}

/*
NewNodeEncodeDecoder returns a NodeEncodeDecoder that uses the
given CriteriaEncodeDecoder to encode/decode nodes' feature criteria.
*/
func NewNodeEncodeDecoder(ced CriteriaEncodeDecoder, features []feature.Feature) NodeEncodeDecoder {
	return &nodeEncodeDecoder{ced, features}
}

func (ned *nodeEncodeDecoder) Encode(n *tree.Node) ([]byte, error) {
	jn := &node{
		ID:       n.ID,
		ParentID: n.ParentID,
	}
	if len(n.SubtreeIDs) > 0 {
		jn.SubtreeIDs = n.SubtreeIDs
	}
	if n.FeatureCriterion != nil {
		fc, err := ned.CriteriaEncodeDecoder.Encode(n.FeatureCriterion)
		if err != nil {
			return nil, err
		}
		rfc := json.RawMessage(fc)
		jn.FeatureCriterion = &rfc
	}
	if n.Prediction != nil {
		p, err := json.Marshal(&jsonPrediction{Probabilities: n.Prediction.Probabilities(), Weight: n.Prediction.Weight()})
		if err != nil {
			return nil, err
		}
		rp := json.RawMessage(p)
		jn.Prediction = &rp
	}
	if n.SubtreeFeature != nil {
		jn.SubtreeFeature = n.SubtreeFeature.Name()
	}
	return json.Marshal(jn)
}

func (ned *nodeEncodeDecoder) Decode(data []byte) (*tree.Node, error) {
	jn := &node{}
	err := json.Unmarshal(data, jn)
	if err != nil {
		return nil, err
	}
	n := &tree.Node{}
	if jn.FeatureCriterion != nil {
		n.FeatureCriterion, err = ned.CriteriaEncodeDecoder.Decode(*jn.FeatureCriterion)
		if err != nil {
			return nil, err
		}
	}
	if jn.Prediction != nil {
		n.Prediction, err = UnmarshalJSONPrediction(*jn.Prediction)
		if err != nil {
			return nil, err
		}
	}
	n.ID = jn.ID
	n.ParentID = jn.ParentID
	if len(jn.SubtreeIDs) > 0 {
		n.SubtreeIDs = jn.SubtreeIDs
	}
	if jn.SubtreeFeature != "" {
		var nf feature.Feature
		for _, f := range ned.features {
			if f.Name() == jn.SubtreeFeature {
				nf = f
				break
			}
		}
		if nf == nil {
			return nil, fmt.Errorf("unmarshalling node %v: unknown feature %v", n.ID, jn.SubtreeFeature)
		}
		n.SubtreeFeature = nf
	}
	return n, nil
}

/*
UnmarshalJSONPrediction takes a slice of bytes and returns
a pointer to a new tree.Prediction with the data from the slice
unmarshalled into it or an error. The slice of bytes is expected
to contain a JSON object with the following fields:
* "probabilities": a JSON object with string keys (values) and
numeric (float64) values (probability of that value)
* "weight": a number (integer) corresponding to the number of
samples in the dataset from which the prediction was made.
*/
func UnmarshalJSONPrediction(b []byte) (*tree.Prediction, error) {
	jp := &jsonPrediction{}
	err := json.Unmarshal(b, jp)
	if err != nil {
		return nil, err
	}
	return tree.NewPrediction(jp.Probabilities, jp.Weight), nil
}
