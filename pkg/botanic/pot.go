package botanic

import "sync"

/*
Pot represents the context in which a tree is grown.

Its Grow method takes a Set and returns a tree that predicts the set
*/
type Pot interface {
	Grow(Set) *Tree
}

type pot struct {
	features       []Feature
	classFeature   Feature
	minimumEntropy float64
	pruner         Pruner
}

/*
New takes a slice of features, a feature class and a pruner and returns a Pot that
uses those to grow a tree
*/
func New(features []Feature, fc Feature, p Pruner) Pot {
	return &pot{features, fc, 0.0, p}
}

func (p *pot) Grow(s Set) *Tree {
	t := &Tree{set: s}
	p.develop(t, p.features)
	return t
}

func (p *pot) develop(t *Tree, features []Feature) {
	t.prediction, _ = newPredictionFromSet(t.set, p.classFeature)
	if len(features) == 0 || t.set.Entropy(p.classFeature) <= p.minimumEntropy {
		return
	}
	var partition *Partition
	var featureIndex int
	for i, f := range features {
		p := p.partition(t.set, f, p.classFeature)
		if partition == nil || (p != nil && p.informationGain > partition.informationGain) {
			partition = p
			featureIndex = i
		}
	}
	if partition == nil {
		return
	}
	t.subtreeFeature = partition.feature
	t.informationGain = partition.informationGain
	t.subtrees = partition.subtrees
	t.undefinedSubtree = NewTreeForUndefinedFeatureCriterion(partition.feature, t.set)
	t.set = nil
	features[featureIndex], features[0] = features[0], features[featureIndex]
	var wg sync.WaitGroup
	wg.Add(len(t.subtrees) + 1)
	for _, subtree := range append(t.subtrees, t.undefinedSubtree) {
		go func(subtree *Tree) {
			p.develop(subtree, append([]Feature{}, features[1:]...))
			wg.Done()
		}(subtree)
	}
	wg.Wait()
	features[featureIndex], features[0] = features[0], features[featureIndex]
}

func (p *pot) partition(s Set, f Feature, cf Feature) *Partition {
	switch f := f.(type) {
	default:
		return nil
	case *DiscreteFeature:
		return NewDiscretePartition(s, f, cf, p.pruner)
	case *ContinuousFeature:
		return NewContinuousPartition(s, f, cf, p.pruner)
	}
}
