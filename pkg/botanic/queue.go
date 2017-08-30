package botanic

import (
	"context"
	"fmt"
	"sync"
)

/*
queue is a queue where tree nodes
are sent to be developed or expanded
in tasks.
*/
type queue struct {
	workers    chan *worker
	tasks      chan *task
	ctx        context.Context
	cancelFunc context.CancelFunc
	results    chan error
	wg         *sync.WaitGroup
	result     chan error
}

/*
worker holds the data for a goroutine
that takes tasks from a queue to
run them and develop tree nodes
*/
type worker struct {
	*queue
	id    string
	tasks chan *task
}

/*
task represents a task enqueued
on a queue to be processed by a worker.
It holds a Pot, a Tree node and a set of
features which allows it to develop the
tree, and also holds an error channel on
which to send the result of the operation.
*/
type task struct {
	pot      *pot
	tree     *Tree
	features []Feature
	result   chan error
}

/*
DefaultMaxConcurrency defines the maximum number
of Tree nodes that will be developed simultaneously
by a Pot.
*/
const DefaultMaxConcurrency = 10

func newQueue(ctx context.Context, workers int) *queue {
	if workers < 1 {
		workers = DefaultMaxConcurrency
	}
	wc := make(chan *worker)
	tasks := make(chan *task)
	ctx, cancelFunc := context.WithCancel(ctx)
	results := make(chan error)
	wg := &sync.WaitGroup{}
	result := make(chan error, 1)
	q := &queue{wc, tasks, ctx, cancelFunc, results, wg, result}
	go q.run()
	go q.processTaskResults()
	for i := 0; i < workers; i++ {
		newWorker(fmt.Sprintf("worker%d", i), q)
	}
	return q
}

func (q *queue) stop() {
	q.cancelFunc()
}

func (q *queue) add(p *pot, tree *Tree, fs []Feature) {
	tvalue := &task{p, tree, fs, q.results}
	q.wg.Add(1)
	go func(t *task) {
		select {
		case <-q.ctx.Done():
			q.wg.Done()
		case q.tasks <- t:
		}
	}(tvalue)
}

func (q *queue) waitForAll() error {
	q.wg.Wait()
	q.stop()
	return <-q.result
}

func (q *queue) run() {
	var finished bool
	for !finished {
		select {
		case <-q.ctx.Done():
			finished = true
		case w := <-q.workers:
			select {
			case t := <-q.tasks:
				go q.assignTask(t, w)
			case <-q.ctx.Done():
				finished = true
			}
		}
	}
}

func (q *queue) assignTask(t *task, w *worker) {
	select {
	case w.tasks <- t:
	case <-q.ctx.Done():
		q.wg.Done()
	}
}

func newWorker(id string, q *queue) *worker {
	w := &worker{q, id, make(chan *task)}
	go w.run()
	return w
}

func (w *worker) run() {
	var finished bool
	select {
	case w.queue.workers <- w:
	case <-w.queue.ctx.Done():
		finished = true
	}
	for !finished {
		select {
		case t := <-w.tasks:
			if t == nil {
				break
			}
			w.process(t)
		case <-w.queue.ctx.Done():
			finished = true
		}
		select {
		case w.queue.workers <- w:
		case <-w.queue.ctx.Done():
			finished = true
		}
	}
}

func (w *worker) process(t *task) {
	err := t.pot.develop(w.queue.ctx, t.tree, t.features, w.queue)
	if err != nil {
		select {
		case <-w.queue.ctx.Done():
		case t.result <- err:
		}
	}
	w.queue.wg.Done()
}

func (q *queue) processTaskResults() {
	var finished bool
	for !finished {
		select {
		case err := <-q.results:
			if err != nil {
				q.stop()
				q.result <- err
				finished = true
			}
		case <-q.ctx.Done():
			finished = true
		}
	}
	close(q.result)
}
