package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Queue represents a queue where tasks to develop
// tree nodes can be pushed and pulled. The idea
// is a worker will use the Pull method to obtain
// a task. It will start processing it and will then
// either complete it or drop it halfway.
//
// All its methods have a context.Context as first
// parameter that implementations may use to allow
// timeouts and cancellations on the Queue operations.
type Queue interface {
	// Push takes a task and stores it in the queue or
	// returns an error. The task will count as pending.
	Push(context.Context, *Task) error
	// Pull returns a task and a context that may have
	// a timeout or allow its cancellation, or an error.
	// The pulled task will be counted as running from
	// then on.
	// If there are no tasks to pull, implementations
	// should not return an error, but 3 nil values.
	// In case of cancellation, workers should still
	// drop the task.
	Pull(context.Context) (*Task, context.Context, error)
	// Drop takes the ID for a tasks an makes it available
	// for pulling from the Queue again. The dropped task
	// should be count by implementations as pending
	// again, unless it has been previously completed.
	// Workers should use this to return to the queue
	// tasks they have not completed.
	Drop(context.Context, string) error
	// Complete takes the ID for a task. Implementations
	// should remove the task from the running state.
	Complete(context.Context, string) error
	// Count returns the number of
	// pending and running tasks in the queue
	// or an error
	Count(context.Context) (int, int, error)
	// Stops the queue. Implementations should use the
	// call to free resources and even cancel pulled
	// contexts.
	Stop(context.Context) error
}

type memQueue struct {
	pendingTasks []*Task
	head         int
	tail         int
	pending      int
	runningTasks map[string]*Task
	lock         *sync.RWMutex
	ctx          context.Context
	ctxCancel    context.CancelFunc
}

// New returns a queue backed only by the process memory
func New() Queue {
	ctx, cancel := context.WithCancel(context.Background())
	return &memQueue{
		runningTasks: make(map[string]*Task),
		lock:         &sync.RWMutex{},
		ctx:          ctx,
		ctxCancel:    cancel,
	}
}

// WaitFor takes a context and a queue and waits for
// all its tasks to have been processed, that is, for
// for the given queue's Count method to return 0, 0, nil.
// It will return a non-nil error if the given context
// times out or is cancelled, or if the queue's Count
// operation returns an error.
// Use this function to wait for the processing of a
// tree once you have started to grow it and have workers
// processing its tasks.
func WaitFor(ctx context.Context, q Queue) error {
	ticker := time.NewTicker(time.Second)
	for {
		running, pending, err := q.Count(ctx)
		if err != nil {
			return err
		}
		if pending+running == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (mq *memQueue) Push(ctx context.Context, t *Task) error {
	return mq.withLock(ctx, func(ctx context.Context) error {
		mq.push(t)
		return nil
	})
}

func (mq *memQueue) Pull(ctx context.Context) (*Task, context.Context, error) {
	var task *Task
	err := mq.withLock(ctx, func(ctx context.Context) error {
		if mq.pending == 0 {
			return nil
		}
		mq.pending--
		task = mq.pendingTasks[mq.head]
		mq.pendingTasks[mq.head] = nil
		mq.head = (mq.head + 1) % len(mq.pendingTasks)
		mq.runningTasks[task.ID()] = task
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if task == nil {
		return nil, nil, nil
	}
	return task, mq.ctx, nil
}

func (mq *memQueue) Drop(ctx context.Context, id string) error {
	return mq.withLock(ctx, func(ctx context.Context) error {
		t, ok := mq.runningTasks[id]
		if !ok {
			return nil
		}
		delete(mq.runningTasks, id)
		mq.push(t)
		return nil
	})
}

func (mq *memQueue) Complete(ctx context.Context, id string) error {
	return mq.withLock(ctx, func(ctx context.Context) error {
		delete(mq.runningTasks, id)
		return nil
	})
}

func (mq *memQueue) Count(ctx context.Context) (int, int, error) {
	var pending, running int
	err := mq.withRLock(ctx, func(ctx context.Context) error {
		pending = mq.pending
		running = len(mq.runningTasks)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return pending, running, nil
}

func (mq *memQueue) Stop(ctx context.Context) error {
	mq.ctxCancel()
	return nil
}

func (mq *memQueue) String() string {
	return fmt.Sprintf("{Queue pending: %d (%v head:%d tail:%d)", mq.pending, mq.pendingTasks, mq.head, mq.tail)
}

func (mq *memQueue) push(t *Task) {
	if mq.pending == len(mq.pendingTasks) {
		mq.reorder()
		mq.pendingTasks = append(mq.pendingTasks, t)
	} else {
		mq.pendingTasks[mq.tail] = t
		mq.tail = (mq.tail + 1) % len(mq.pendingTasks)
	}
	mq.pending++
}

func (mq *memQueue) reorder() {
	if mq.head == 0 {
		return
	}
	mq.pendingTasks = append(mq.pendingTasks[mq.head:], mq.pendingTasks[0:mq.head]...)
	mq.head = 0
	mq.tail = mq.pending % len(mq.pendingTasks)
}

func (mq *memQueue) withLock(ctx context.Context, f func(ctx context.Context) error) error {
	gotLock := make(chan struct{})
	go func() {
		mq.lock.Lock()
		select {
		case <-ctx.Done():
			mq.lock.Unlock()
		case gotLock <- struct{}{}:
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gotLock:
		defer mq.lock.Unlock()
	}
	return f(ctx)
}

func (mq *memQueue) withRLock(ctx context.Context, f func(ctx context.Context) error) error {
	gotLock := make(chan struct{})
	go func() {
		mq.lock.RLock()
		select {
		case <-ctx.Done():
			mq.lock.RUnlock()
		case gotLock <- struct{}{}:
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gotLock:
		defer mq.lock.RUnlock()
	}
	return f(ctx)
}
