package json

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/pbanos/botanic/feature"
)

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

type jsonCriteriaEncodeDecoder []feature.Feature

type jsonCriterion struct {
	Type    string `json:"t"`
	Feature string `json:"f"`
	Value   string `json:"v,omitempty"`
	A       string `json:"a,omitempty"`
	B       string `json:"b,omitempty"`
}

// NewCriteriaEncodeDecoder takes a slice of feature.Feature and returns a
// CriteriaEncodeDecoder that marshals and unmarshals
// criteria into/from slices of byts as JSON.
// Specifically, criteria are encoded as a JSON object
// with a "feature" property set to the name of the feature
// of the criteria and a "type" property that can be one of
// "continuous", "discrete" or "undefined":
//  * If the criteria is continuous it will have "a" and "b"
//  properties defining the start and end of the interval for
//  the feature
//  * If the criteria is discrete it will have a "value"
//  property defining the specific value for the feature
//  * If the criteria is undefined ti will have no additional
//  properties
func NewCriteriaEncodeDecoder(features []feature.Feature) CriteriaEncodeDecoder {
	return jsonCriteriaEncodeDecoder(features)
}

func (jced jsonCriteriaEncodeDecoder) Encode(fc feature.Criterion) ([]byte, error) {
	switch c := fc.(type) {
	case feature.ContinuousCriterion:
		return jced.encodeContinuousCriterion(c)
	case feature.DiscreteCriterion:
		return jced.encodeDiscreteCriterion(c)
	case feature.UndefinedCriterion:
		return jced.encodeUndefinedCriterion(c)
	default:
		return nil, fmt.Errorf("unknown type of feature.Criterion %T", fc)
	}
}
func (jced jsonCriteriaEncodeDecoder) Decode(data []byte) (feature.Criterion, error) {
	jc := &jsonCriterion{}
	err := json.Unmarshal(data, jc)
	if err != nil {
		return nil, err
	}
	return jc.Criterion(jced)
}

func (jced *jsonCriteriaEncodeDecoder) encodeContinuousCriterion(cfc feature.ContinuousCriterion) ([]byte, error) {
	a, b := cfc.Interval()
	sa := fmt.Sprintf("%f", a)
	sb := fmt.Sprintf("%f", b)
	return json.Marshal(&jsonCriterion{
		Type:    "continuous",
		Feature: cfc.Feature().Name(),
		A:       sa,
		B:       sb,
	})
}

func (jced *jsonCriteriaEncodeDecoder) encodeDiscreteCriterion(dfc feature.DiscreteCriterion) ([]byte, error) {
	return json.Marshal(&jsonCriterion{
		Type:    "discrete",
		Feature: dfc.Feature().Name(),
		Value:   dfc.Value(),
	})
}

func (jced *jsonCriteriaEncodeDecoder) encodeUndefinedCriterion(u feature.UndefinedCriterion) ([]byte, error) {
	return json.Marshal(&jsonCriterion{
		Type:    "undefined",
		Feature: u.Feature().Name(),
	})
}

func (jc *jsonCriterion) Criterion(features []feature.Feature) (feature.Criterion, error) {
	var f feature.Feature
	for _, feat := range features {
		if feat.Name() == jc.Feature {
			f = feat
			break
		}
	}
	if f == nil {
		return nil, fmt.Errorf("unknown feature '%s'", jc.Feature)
	}
	switch jc.Type {
	case "continuous":
		return jc.toContinuousCriterion(f)
	case "discrete":
		return jc.toDiscreteCriterion(f)
	case "undefined":
		return jc.toUndefinedCriterion(f)
	}
	return nil, fmt.Errorf("unknown feature criterion type '%s'", jc.Type)
}

func (jc *jsonCriterion) toUndefinedCriterion(f feature.Feature) (feature.Criterion, error) {
	return feature.NewUndefinedCriterion(f), nil
}

func (jc *jsonCriterion) toDiscreteCriterion(f feature.Feature) (feature.Criterion, error) {
	df, ok := f.(*feature.DiscreteFeature)
	if !ok {
		return nil, fmt.Errorf("expected discrete feature for discrete criterion but found %T feature %v", f, f.Name())
	}
	return feature.NewDiscreteCriterion(df, jc.Value), nil
}

func (jc *jsonCriterion) toContinuousCriterion(f feature.Feature) (feature.Criterion, error) {
	cf, ok := f.(*feature.ContinuousFeature)
	if !ok {
		return nil, fmt.Errorf("expected continuous feature for continuous criterion but found %T feature %v", f, f.Name())
	}
	var a, b float64
	var err error
	if jc.A == "-Inf" {
		a = math.Inf(-1)
	} else {
		a, err = strconv.ParseFloat(jc.A, 64)
		if err != nil {
			return nil, err
		}
	}
	if jc.B == "+Inf" {
		b = math.Inf(+1)
	} else {
		b, err = strconv.ParseFloat(jc.B, 64)
		if err != nil {
			return nil, err
		}
	}
	return feature.NewContinuousCriterion(cf, a, b), nil
}
