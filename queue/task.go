package queue

import (
	"fmt"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/tree"
)

// Task represents a tree.Node to be developed
// on a tree.Tree.
type Task struct {
	// The node to be developed
	Node *tree.Node
	// The dataset of training data with samples
	// satisfying the constraints on the node
	// and its ancestors.
	Dataset dataset.Dataset
	// The list of features that can be used
	// to split the node into branches.
	// It should exclude the features used in
	// ancestor nodes.
	AvailableFeatures []feature.Feature
}

// ID returns a string that identifies the
// task, the ID of its Node.
func (t *Task) ID() string {
	return t.Node.ID
}

func (t *Task) String() string {
	return fmt.Sprintf("{Task %s}", t.Node.ID)
}
