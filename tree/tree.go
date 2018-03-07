package tree

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
)

// Tree represents a a regression tree. It is composed of a
// NodeStore where all its nodes are stored, the id for the
// root node of the tree and the label it is able to
// predict.
type Tree struct {
	NodeStore
	RootID string
	Label  feature.Feature
}

// New takes the ID for the root Node, a NodeStore and a label feature and
// returns a tree composed of the nodes in the NodeStore connected to the
// node with the given root ID that to predict the given feature.
func New(rootID string, nodeStore NodeStore, label feature.Feature) *Tree {
	return &Tree{nodeStore, rootID, label}
}

// Predict takes a sample and returns a prediction according to the tree and an
// error if the prediction could not be made.
func (t *Tree) Predict(ctx context.Context, s feature.Sample) (*Prediction, error) {
	if t == nil {
		return nil, fmt.Errorf("nil tree cannot predict samples")
	}
	n, err := t.Get(ctx, t.RootID)
	if err != nil {
		return nil, fmt.Errorf("predicting sample: retrieving node %v: %v", t.RootID, err)
	}
	if n == nil {
		return nil, fmt.Errorf("predicting sample: root node %v not found", t.RootID)
	}
	for {
		if n.SubtreeFeature == nil {
			break
		}
		var selectedNode *Node
		for _, nID := range n.SubtreeIDs {
			subnode, err := t.Get(ctx, nID)
			if err != nil {
				return nil, fmt.Errorf("predicting sample: retrieving node %v: %v", nID, err)
			}
			if subnode == nil {
				return nil, fmt.Errorf("predicting sample: node %v not found", nID)
			}
			if subnode.FeatureCriterion != nil {
				ok, err := subnode.FeatureCriterion.SatisfiedBy(s)
				if err != nil {
					return nil, err
				}
				if ok {
					selectedNode = subnode
					if _, ok = subnode.FeatureCriterion.(feature.UndefinedCriterion); !ok {
						break
					}
				}
			}
		}
		if selectedNode == nil {
			return nil, fmt.Errorf("sample does not satisfy any subtree criteria on feature %s", n.SubtreeFeature.Name())
		}
		n = selectedNode
	}
	if n.Prediction != nil {
		return n.Prediction, nil
	}
	return nil, ErrCannotPredictFromSample
}

/*
Test takes a context.Context, a Set and a class Feature and returns three values:
 * the prediction success rate of the tree over the given Set for the label
 * the number of failing predictions for the dataset because of ErrCannotPredictFromSample errors
 * an error if a prediction could not be set for reasons other than the tree not
   being able to do so. If this is not nil, the other values will be 0.0 and 0
   respectively
*/
func (t *Tree) Test(ctx context.Context, s dataset.Dataset) (float64, int, error) {
	if t == nil {
		return 0.0, 0, nil
	}
	var result float64
	var errCount int
	samples, err := s.Samples(ctx)
	if err != nil {
		return 0.0, 0, err
	}
	count, err := s.Count(ctx)
	if err != nil {
		return 0.0, 0, err
	}
	for _, sample := range samples {
		p, err := t.Predict(ctx, sample)
		if err != nil {
			if err != ErrCannotPredictFromSample {
				return 0.0, 0, err
			}
			errCount++
		} else {
			pV, _ := p.PredictedValue()
			v, err := sample.ValueFor(t.Label)
			if err != nil {
				return 0.0, 0, err
			}
			if pV == v {
				result += 1.0
			}
		}
	}
	result = result / float64(count)
	return result, errCount, nil
}

// Traverse takes a context, bottomup boolean and an
// error-returning function that takes a context and a node
// as parameters, and goes through the tree running the
// function with the context and every traversed node.
// Traverse will call the function with a parent node before
// calling it for its children if bottomup is false, and
// call it after its children if bottomup is true.
// If the given context times out or is cancelled, the context
// error is returned. If a node cannot be retrieved from the
// tree's node store, the obtained error is returned. If the
// call to the function returns an error, the traversing is
// aborted and the error is returned. Otherwise, when the
// traversing is over, nil is returned.
func (t *Tree) Traverse(ctx context.Context, bottomup bool, f func(context.Context, *Node) error) error {
	n, err := t.NodeStore.Get(ctx, t.RootID)
	if err != nil {
		return err
	}
	return t.traverse(ctx, n, bottomup, f)
}

func (t *Tree) traverse(ctx context.Context, n *Node, bottomup bool, f func(context.Context, *Node) error) error {
	err := ctx.Err()
	if err != nil {
		return err
	}
	if !bottomup {
		err = f(ctx, n)
	}
	if err != nil {
		return err
	}
	for _, snID := range n.SubtreeIDs {
		sn, err := t.NodeStore.Get(ctx, snID)
		if err != nil {
			return err
		}
		err = t.traverse(ctx, sn, bottomup, f)
		if err != nil {
			return err
		}
	}
	if bottomup {
		err = f(ctx, n)
	}
	if err != nil {
		return err
	}
	return nil
}

func (t *Tree) String() string {
	return t.subtreeString(t.RootID)
}

func (t *Tree) subtreeString(nodeID string) string {
	n, err := t.NodeStore.Get(context.TODO(), nodeID)
	if err != nil {
		return fmt.Sprintf("ERROR: %s\n", err.Error())
	}
	result := fmt.Sprintf("[%s]\n", nodeID)
	if n.FeatureCriterion != nil {
		result = fmt.Sprintf("%s{ %v }\n", result, n.FeatureCriterion)
	}
	if n.Prediction != nil {
		result = fmt.Sprintf("%s{ %v }\n", result, n.Prediction)
	}
	if len(n.SubtreeIDs) > 0 {
		result = fmt.Sprintf("%s|\n", result)
	} else {
		result = fmt.Sprintf("%s \n", result)
	}
	for i, subtreeID := range n.SubtreeIDs {
		for j, line := range strings.Split(t.subtreeString(subtreeID), "\n") {
			if len(line) > 0 {
				if j == 0 {
					result = fmt.Sprintf("%s|__%s\n", result, line)
				} else {
					if i == len(n.SubtreeIDs)-1 {
						result = fmt.Sprintf("%s   %s\n", result, line)
					} else {
						result = fmt.Sprintf("%s|  %s\n", result, line)
					}
				}
			}
		}
	}
	return result
}
