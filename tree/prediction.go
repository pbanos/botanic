package tree

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

/*
Prediction represents a prediction made by a decission regression Tree
*/
type Prediction struct {
	probabilities map[string]float64
	weight        int
}

// PredictionError represents an error related with predictions
type PredictionError string

/*
ErrCannotPredictFromSample is the error returned by the Predict method of a tree
when the prediction cannot be made because the tree itself cannot make
a prediction for that kind of sample, as opposed to cases where values
for a feature cannot be obtained for example.
*/
const ErrCannotPredictFromSample = PredictionError("no prediction available for this kind of sample")

/*
ErrCannotPredictFromEmptySet is the error returned when trying to build a prediction
based on an empty dataset.
*/
const ErrCannotPredictFromEmptySet = PredictionError("cannot make prediction for empty dataset")

func (pe PredictionError) Error() string {
	return string(pe)
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
Probabilities returns a map of string to float64 containing
the probabilities of each available value
*/
func (p *Prediction) Probabilities() map[string]float64 {
	return p.probabilities
}

/*
Weight returns the weight of the prediction: an
int equal to the number of samples in the dataset from which
the prediction was made
*/
func (p *Prediction) Weight() int {
	return p.weight
}

/*
NewPrediction takes a map[string]float64 with the probabilities
of each value in the prediction and an integer with the number
of samples in the dataset from which those probabilities were computed
and returns a prediction representing those values.
*/
func NewPrediction(probs map[string]float64, weight int) *Prediction {
	return &Prediction{probabilities: probs, weight: weight}
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

func joinPredictions(p1 *Prediction, p2 *Prediction) (*Prediction, error) {
	totalWeight := p1.weight + p2.weight
	if totalWeight == 0 {
		return nil, ErrCannotPredictFromEmptySet
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

// NewPredictionFromSet takes a context, a dataset and a feature and returns
// a prediction for the feature based on the (training) data in the dataset
// or an error if there are no samples in the dataset, or the dataset cannot
// be queried
func NewPredictionFromSet(ctx context.Context, s dataset.Dataset, f feature.Feature) (*Prediction, error) {
	weight, err := s.Count(ctx)
	if err != nil {
		return nil, err
	}
	if weight == 0 {
		return nil, ErrCannotPredictFromEmptySet
	}
	probs := make(map[string]float64)
	fvc, err := s.CountFeatureValues(ctx, f)
	if err != nil {
		return nil, err
	}
	for v, c := range fvc {
		probs[v] = float64(c) / float64(weight)
	}
	return &Prediction{probs, weight}, nil
}
