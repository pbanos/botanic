package botanic

import (
	"context"
	"math/rand"
	"time"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/queue"
	"github.com/pbanos/botanic/tree"
)

// Seed takes a context, a label feature, a slice of features,
// a dataset, a queue and a node store and sets everything
// up so that workers that consume from the queue afterwards
// grow a tree that predicts the given label feature using
// the features in the given slice and according to the training
// data on the given dataset.
// Specifically it will create the root node of the tree on the
// node store and push a task to branch it out on the queue.
// The function returns the tree that can be grown or an error
// if the node cannot be created on the store, or the task pushed
// to the queue (in the amount of time allowed by the given
// context).
func Seed(ctx context.Context, label feature.Feature, features []feature.Feature, s dataset.Dataset, q queue.Queue, ns tree.NodeStore) (*tree.Tree, error) {
	n := &tree.Node{}
	err := ns.Create(ctx, n)
	if err != nil {
		return nil, err
	}
	task := &queue.Task{Node: n, Dataset: s, AvailableFeatures: features}
	t := tree.New(n.ID, ns, label)
	err = q.Push(ctx, task)
	if err != nil {
		ns.Delete(ctx, n)
		return nil, err
	}
	return t, nil
}

// BranchOut takes a context, a task, a tree and a pruning strategy,
// develops the node in the task using the task's dataset and available
// feature to predict the tree's label feature and returns a set of
// tasks to develop the resulting children nodes or an error.
func BranchOut(ctx context.Context, task *queue.Task, t *tree.Tree, ps *PruningStrategy) (tasks []*queue.Task, e error) {
	prediction, err := tree.NewPredictionFromSet(ctx, task.Dataset, t.Label)
	if err != nil {
		if err != tree.ErrCannotPredictFromEmptySet {
			return nil, err
		}
	}
	defer func() {
		err = t.NodeStore.Store(ctx, task.Node)
		if e == nil {
			e = err
		}
	}()
	task.Node.Prediction = prediction
	sEntropy, err := task.Dataset.Entropy(ctx, t.Label)
	if err != nil {
		return nil, err
	}
	if len(task.AvailableFeatures) == 0 || sEntropy <= ps.MinimumEntropy {
		return nil, nil
	}
	shuffleFeatures(task.AvailableFeatures)
	var selectedPartition *Partition
	var featureIndex int
	for i, f := range task.AvailableFeatures {
		part, err := partition(ctx, task.Dataset, f, t.Label, ps)
		if err != nil {
			return nil, err
		}
		if selectedPartition == nil || (part != nil && part.informationGain > selectedPartition.informationGain) {
			selectedPartition = part
			featureIndex = i
		}
	}
	if selectedPartition == nil {
		return nil, nil
	}
	task.Node.SubtreeFeature = selectedPartition.Feature
	stAvailableFeatures := make([]feature.Feature, 0, len(task.AvailableFeatures)-1)
	for fi, sf := range task.AvailableFeatures {
		if fi != featureIndex {
			stAvailableFeatures = append(stAvailableFeatures, sf)
		}
	}
	stNodeIDs := make([]string, 0, len(selectedPartition.Tasks))
	for _, st := range selectedPartition.Tasks {
		st.Node.ParentID = task.Node.ID
		err = t.NodeStore.Create(ctx, st.Node)
		if err != nil {
			return nil, err
		}
		stNodeIDs = append(stNodeIDs, st.Node.ID)
		st.AvailableFeatures = stAvailableFeatures
	}
	task.Node.SubtreeIDs = stNodeIDs
	return selectedPartition.Tasks, nil
}

// Work takes a context, a tree, a queue, a pruning strategy
// and an emptyQueueSleep duration and enters a loop in which
// it:
//   * pulls a task for the queue,
//   * branches its node out into new subnodes using BranchOut
//   * pushes the tasks for the new subnodes into the queue
//   * marks the task as completed on the queue
//
// If at some point no task can be pulled from the queue and
// the sum of tasks running and pending on the queue is 0, the
// worker ends returning nil. If no task can be pulled but the
// sum is not 0, then the worker will sleep for the given
// emptyQueueSleep duration and then retry.
//
// Work will return a non-nil error if the given context
// times out or is cancelled, if BranchOut returns a non-nil
// error or if an operation with the given queue returns a
// non-nil error.
func Work(ctx context.Context, t *tree.Tree, q queue.Queue, ps *PruningStrategy, emptyQueueSleep time.Duration) error {
	for {
		task, tctx, tcf, err := q.Pull(ctx)
		if err != nil {
			return err
		}
		if task == nil {
			r, p, err := q.Count(ctx)
			if err != nil {
				return err
			}
			if r+p == 0 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(emptyQueueSleep):
			}
			continue
		}
		mctx, cancel := mergeCtxCancel(tctx, ctx)
		err = workTask(mctx, task, t, q, ps)
		cancel()
		tcf()
		if err != nil {
			return err
		}
		err = ctx.Err()
		if err != nil {
			return err
		}
	}
	return nil
}

func workTask(ctx context.Context, task *queue.Task, t *tree.Tree, q queue.Queue, ps *PruningStrategy) error {
	defer func() {
		q.Drop(ctx, task.ID())
	}()
	tasks, err := BranchOut(ctx, task, t, ps)
	if err != nil {
		return err
	}
	for _, st := range tasks {
		err = q.Push(ctx, st)
		if err != nil {
			return err
		}
	}
	return q.Complete(ctx, task.ID())
}

func mergeCtxCancel(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	mctx, cancel := context.WithCancel(ctx1)
	go func() {
		select {
		case <-mctx.Done():
		case <-ctx2.Done():
			cancel()
		}
	}()
	return mctx, cancel
}

func shuffleFeatures(features []feature.Feature) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for len(features) > 0 {
		n := len(features)
		randIndex := r.Intn(n)
		features[n-1], features[randIndex] = features[randIndex], features[n-1]
		features = features[:n-1]
	}
}
