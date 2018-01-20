package tree

import (
	"github.com/pbanos/botanic/feature"
)

/*
Node is a node of the tree
*/
type Node struct {
	// An ID to identify the node
	ID string
	// The ID for the parent of the node in the tree
	ParentID string
	// An slice with the IDs of the nodes directly under this node
	SubtreeIDs []string
	// The prediction for samples that satisfied node constraints from the root of the
	// tree up to this node.
	Prediction *Prediction
	// The constraint this node imposes on samples.
	// For growing trees it is the criterion that applied to the parent node's set produces
	// this node's set.
	// For fully-grown trees it is the constraint on the evaluated feature that when
	// satisfied by the evaluated sample selects the current node to continue predicting a
	// a sample or testing the tree against it (unless it is an undefined feature constraint,
	// then it should be the last criterion against which to test).
	FeatureCriterion feature.Criterion
	// The feature on which nodes directly under this node must impose a constraint.
	// For growing trees it is the feature that splits the data into sets for the nodes
	// below, whereas for fully-grown trees it is the feature to ask about next on the
	// sample being predicted or tested against.
	SubtreeFeature feature.Feature
}
