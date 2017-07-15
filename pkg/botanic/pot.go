package botanic

import "fmt"

/*
Pot represents the context in which a tree is grown.

Its Grow method takes a Set and returns a tree that predicts the set
*/
type Pot interface {
	Grow(Set) (*Tree, error)
}

type pot struct {
	features       []Feature
	classFeature   Feature
	minimumEntropy float64
	pruner         Pruner
	maxConcurrency int
}

/*
New takes a slice of features, a feature class and a pruner and returns a Pot that
uses those to grow a tree
*/
func New(features []Feature, fc Feature, p Pruner, maxConcurrency int) Pot {
	return &pot{features, fc, 0.0, p, maxConcurrency}
}

func (p *pot) Grow(s Set) (*Tree, error) {
	t := &Tree{set: s}
	q := newQueue(p.maxConcurrency)
	q.add(p, t, p.features)
	err := q.waitForAll()
	return t, err
}

func (p *pot) develop(t *Tree, features []Feature, q *queue) error {
	prediction, err := newPredictionFromSet(t.set, p.classFeature)
	if err != nil {
		if err != ErrCannotPredictFromEmptySet {
			return err
		}
	}
	t.prediction = prediction
	sEntropy, err := t.set.Entropy(p.classFeature)
	if err != nil {
		return err
	}
	if len(features) == 0 || sEntropy <= p.minimumEntropy {
		return nil
	}
	var partition *Partition
	var featureIndex int
	for i, f := range features {
		p, err := p.partition(t.set, f, p.classFeature)
		if err != nil {
			return err
		}
		if partition == nil || (p != nil && p.informationGain > partition.informationGain) {
			partition = p
			featureIndex = i
		}
	}
	if partition == nil {
		return nil
	}
	t.subtreeFeature = partition.feature
	t.informationGain = partition.informationGain
	t.subtrees = partition.subtrees
	subtreeFeatures := make([]Feature, 0, len(features)-1)
	for fi, sf := range features {
		if fi != featureIndex {
			subtreeFeatures = append(subtreeFeatures, sf)
		}
	}
	for _, subtree := range t.subtrees {
		q.add(p, subtree, subtreeFeatures)
	}
	t.undefinedSubtree = NewTreeForUndefinedFeatureCriterion(partition.feature, t.set)
	t.set = nil
	q.add(p, t.undefinedSubtree, subtreeFeatures)
	return nil
}

func (p *pot) partition(s Set, f Feature, cf Feature) (*Partition, error) {
	switch f := f.(type) {
	default:
		return nil, fmt.Errorf("unknown feature type %T", f)
	case *DiscreteFeature:
		return NewDiscretePartition(s, f, cf, p.pruner)
	case *ContinuousFeature:
		return NewContinuousPartition(s, f, cf, p.pruner)
	}
}
