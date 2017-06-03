package botanic

import (
	"encoding/json"
	"fmt"
	"strings"
)

/*
Tree represents a decision regression tree or subtree thereof
*/
type Tree struct {
	subtrees         []*Tree
	undefinedSubtree *Tree
	set              Set
	featureCriterion FeatureCriterion
	subtreeFeature   Feature
	informationGain  float64
	prediction       *Prediction
}

type jsonTree struct {
	Subtrees         []*Tree          `json:"subtrees,omitempty"`
	UndefinedSubtree *Tree            `json:"undefinedSubtree,omitempty"`
	FeatureCriterion *json.RawMessage `json:"featureCriterion,omitempty"`
	SubtreeFeature   string           `json:"subtreeFeature,omitempty"`
	Prediction       *Prediction      `json:"prediction,omitempty"`
}

/*
Prediction represents a prediction made by a decission regression Tree
*/
type Prediction struct {
	probabilities map[string]float64
	weight        int
}

type jsonPrediction struct {
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Weight        int                `json:"weight,omitempty"`
}

/*
NewTreeFromFeatureCriterion takes a FeatureCriterion, a set of data and a class Feature
and returns a non-developed Tree for the subset of data satisfying the FeatureCriterion.
*/
func NewTreeFromFeatureCriterion(fc FeatureCriterion, s Set) *Tree {
	return &Tree{
		set:              s.SubsetWith(fc),
		featureCriterion: fc,
	}
}

/*
NewTreeForUndefinedFeatureCriterion takes a Feature, a set of data and a class Feature
and returns a non-developed Tree with an UndefinedFeatureCriterion, for the case the given
Feature is undefined.
*/
func NewTreeForUndefinedFeatureCriterion(f Feature, s Set) *Tree {
	return &Tree{
		set:              s,
		featureCriterion: NewUndefinedFeatureCriterion(f),
	}
}

/*
ProbabilityOf takes a string value and returns the float64 probability of that
value according to the prediction.
*/
func (p *Prediction) ProbabilityOf(value string) float64 {
	return p.probabilities[value]
}

func (p *Prediction) String() string {
	return strings.Replace(fmt.Sprintf("%v", p.probabilities), "map", "", 1)
}

/*
PredictedValue returns a string with the most probable value and a float64 with
its prevalence
*/
func (p *Prediction) PredictedValue() (value string, prob float64) {
	for k, v := range p.probabilities {
		if v > prob {
			value = k
			prob = v
		}
	}
	return
}

/*
Predict takes a sample and returns a prediction according to the tree and an
error if the prediction could not be made.
*/
func (t *Tree) Predict(s Sample) (*Prediction, error) {
	if t.subtreeFeature == nil {
		if t.prediction != nil {
			return t.prediction, nil
		}
		return nil, fmt.Errorf("no prediction available for this kind of sample")
	}
	if t.undefinedSubtree != nil && s.ValueFor(t.subtreeFeature) == nil {
		return t.undefinedSubtree.Predict(s)
	}
	var prediction *Prediction
	for _, subtree := range t.subtrees {
		if subtree.featureCriterion != nil && subtree.featureCriterion.SatisfiedBy(s) {
			subtreePrediction, err := subtree.Predict(s)
			if err != nil {
				return nil, err
			}
			if prediction == nil {
				prediction = subtreePrediction
			} else {
				prediction, err = joinPredictions(prediction, subtreePrediction)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if prediction == nil {
		return nil, fmt.Errorf("sample does not satisfy any subtree criteria on feature %s", t.subtreeFeature.Name())
	}
	return prediction, nil
}

func joinPredictions(p1 *Prediction, p2 *Prediction) (*Prediction, error) {
	totalWeight := p1.weight + p2.weight
	if totalWeight == 0 {
		return nil, fmt.Errorf("cannot join weightless predictions")
	}
	relativeWeight := float64(p1.weight) / float64(totalWeight)
	mergedProbs := make(map[string]float64)
	for c, p := range p1.probabilities {
		mergedProbs[c] = relativeWeight * p
	}
	relativeWeight = float64(p2.weight) / float64(totalWeight)
	for c, p := range p2.probabilities {
		mergedProbs[c] += relativeWeight * p
	}
	return &Prediction{mergedProbs, totalWeight}, nil
}

func newPredictionFromSet(s Set, f Feature) (*Prediction, error) {
	weight := s.Count()
	if weight == 0 {
		return nil, fmt.Errorf("cannot make prediction for empty set")
	}
	probs := make(map[string]float64)
	count := 0.0
	for _, sample := range s.Samples() {
		if v := sample.ValueFor(f); v != nil {
			vString := fmt.Sprintf("%v", v)
			count += 1.0
			probs[vString] += 1.0
		}
	}
	for k, v := range probs {
		probs[k] = v / count
	}
	return &Prediction{probs, weight}, nil
}

func (t *Tree) String() string {
	var result string
	if t.featureCriterion != nil {
		result = fmt.Sprintf("%s{ %v }\n", result, t.featureCriterion)
	}
	if t.prediction != nil {
		result = fmt.Sprintf("%s{ %v }\n", result, t.prediction)
	}
	if t.set != nil {
		result = fmt.Sprintf("%s[ %v ]\n", result, t.set.Count())
	}
	if t.informationGain != 0.0 {
		result = fmt.Sprintf("%s{ informationGain=%f }\n", result, t.informationGain)
	}
	if len(t.subtrees) > 0 {
		result = fmt.Sprintf("%s|\n", result)
	} else {
		result = fmt.Sprintf("%s \n", result)
	}
	for _, subtree := range t.subtrees {
		for j, line := range strings.Split(subtree.String(), "\n") {
			if len(line) > 0 {
				if j == 0 {
					result = fmt.Sprintf("%s|__%s\n", result, line)
				} else {
					if t.undefinedSubtree == nil {
						result = fmt.Sprintf("%s   %s\n", result, line)
					} else {
						result = fmt.Sprintf("%s|  %s\n", result, line)
					}
				}
			}
		}
	}
	if t.undefinedSubtree != nil {
		for j, line := range strings.Split(t.undefinedSubtree.String(), "\n") {
			if len(line) > 0 {
				if j == 0 {
					result = fmt.Sprintf("%s|__%s\n", result, line)
				} else {
					result = fmt.Sprintf("%s   %s\n", result, line)
				}
			}
		}
	}
	return result
}

/*
MarshalJSON returns a slice of bytes with the Tree serialized to JSON and an error.
A Tree is serialized recursively, with each node consisting of the following
properties:
  * "subtrees": an array with the different subtrees for features criterion if any
  (excluding the subtree for the undefined feature criterion)
  * "undefinedSubtree": a subtree for the undefined feature criterion if available
  * "featureCriterion": the feature criterion for the node
  * "subtreeFeature": the feature for the subtrees of the node, that is, the feature
  that is dividing the data
  * "prediction": the prediction of the classFeature at this point in the tree
*/
func (t *Tree) MarshalJSON() ([]byte, error) {
	var subtreeFeatureName string
	if t.subtreeFeature != nil {
		subtreeFeatureName = t.subtreeFeature.Name()
	}
	jt := &jsonTree{
		Subtrees:         t.subtrees,
		UndefinedSubtree: t.undefinedSubtree,
		SubtreeFeature:   subtreeFeatureName,
		Prediction:       t.prediction,
	}
	if t.featureCriterion != nil {
		fc, err := json.Marshal(t.featureCriterion)
		if err != nil {
			return nil, err
		}
		rfc := json.RawMessage(fc)
		jt.FeatureCriterion = &rfc
	}
	return json.Marshal(jt)
}

/*
UnmarshalJSON takes a slice of bytes containing a serialized Tree and
loads its data. The slice of bytes is expected to have the tree serialized
in JSON format as generated by MarshalJSON
*/
func (t *Tree) UnmarshalJSON(b []byte) error {
	jt := &jsonTree{}
	err := json.Unmarshal(b, &jt)
	if err != nil {
		return err
	}
	if jt.FeatureCriterion != nil {
		jfc := &jsonFeatureCriterion{}
		err = json.Unmarshal(*jt.FeatureCriterion, jfc)
		if err != nil {
			return err
		}
		t.featureCriterion, err = jfc.FeatureCriterion()
		if err != nil {
			return err
		}
	}
	t.subtrees = jt.Subtrees
	t.undefinedSubtree = jt.UndefinedSubtree
	if jt.SubtreeFeature != "" {
		t.subtreeFeature = &UndefinedFeature{jt.SubtreeFeature}
	}
	t.prediction = jt.Prediction
	return nil
}

/*
MarshalJSON returns a slice of bytes with the prediction serialized in JSON and an error.
The serialized prediction object has the following properties
  * probabilities
  * weight
*/
func (p *Prediction) MarshalJSON() ([]byte, error) {
	jp := &jsonPrediction{
		p.probabilities,
		p.weight,
	}
	return json.Marshal(jp)
}

/*
UnmarshalJSON takes a slice of bytes containing a serialized prediction in JSON
and loads its data. The slice of bytes is expected to have the prediction serialized
in JSON format as generated by MarshalJSON
*/
func (p *Prediction) UnmarshalJSON(b []byte) error {
	jp := &jsonPrediction{}
	err := json.Unmarshal(b, &jp)
	if err != nil {
		return err
	}
	p.probabilities = jp.Probabilities
	p.weight = jp.Weight
	return nil
}

/*
Test takes a Set and a class Feature and returns two values:
 * the prediction success rate of the tree over the given Set for the classFeature
 * the number of failing predictions for the set
*/
func (t *Tree) Test(s Set, classFeature Feature) (float64, int) {
	if t == nil {
		return 0.0, 0
	}
	var result float64
	var errCount int
	for _, sample := range s.Samples() {
		p, err := t.Predict(sample)
		if err != nil {
			errCount++
		} else {
			pV, _ := p.PredictedValue()
			if pV == sample.ValueFor(classFeature) {
				result += 1.0
			}
		}
	}
	result = result / float64(s.Count())
	return result, errCount
}
