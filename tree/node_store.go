package tree

import (
	"context"
	"fmt"
	"sync"
)

/*
NodeStore is an interface to manage a store
where nodes can be created, retrieved, updated
and deleted.

All it methods take a context that may allow
cancelling the operation (thus forcing the return
of an error) if the implementation allows it.
*/
type NodeStore interface {
	// Create takes a node and stores it for the
	// first time in the store, creating an ID for
	// it and setting it for the node. It returns
	// an error if the node cannot be stored.
	Create(ctx context.Context, n *Node) error
	// Get takes an id and returns the node in the
	// store with that id (or nil if it cannot be
	// found) or an error if the store cannot be
	// queried
	Get(ctx context.Context, id string) (*Node, error)
	// Store takes a node already existing in the store
	// and updates it on the store. It expect the node
	// to have an ID which it will not alter. It returns
	// an error if the update cannot be performed.
	Store(ctx context.Context, n *Node) error
	// Delete takes a node already existing in the store
	// and deletes it on the store. It returns an error
	// if the node exist but the deletion cannot be
	// performed.
	Delete(ctx context.Context, n *Node) error
	// Close closes the store, implementations should
	// freeing any resources in use as well as ensure
	// any pending changes are applied before returning
	// (unless the context expires). It returns an error
	// if the Close cannot be completed (because of the
	// context or another error)
	Close(ctx context.Context) error
}

type memoryNodeStore struct {
	nodes  map[string]*Node
	lock   *sync.RWMutex
	nextID uint64
}

// NewMemoryNodeStore returns an implementation
// of NodeStore with the process memory space
// as underlying backend
func NewMemoryNodeStore() NodeStore {
	return &memoryNodeStore{
		nodes: make(map[string]*Node),
		lock:  &sync.RWMutex{},
	}
}

func (mns *memoryNodeStore) Create(ctx context.Context, n *Node) error {
	return mns.withLock(ctx, func(ctx context.Context) error {
		taken := true
		for taken {
			if err := ctx.Err(); err != nil {
				return err
			}
			n.ID = mns.generateRandomNodeID(n.ParentID)
			_, taken = mns.nodes[n.ID]
		}
		mns.nodes[n.ID] = n
		return nil
	})
}

func (mns *memoryNodeStore) Store(ctx context.Context, n *Node) error {
	return mns.withLock(ctx, func(ctx context.Context) error {
		mns.nodes[n.ID] = n
		return nil
	})
}

func (mns *memoryNodeStore) Get(ctx context.Context, id string) (*Node, error) {
	var n *Node
	err := mns.withRLock(ctx, func(ctx context.Context) error {
		n = mns.nodes[id]
		return nil
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}
func (mns *memoryNodeStore) Delete(ctx context.Context, n *Node) error {
	return mns.withLock(ctx, func(ctx context.Context) error {
		delete(mns.nodes, n.ID)
		return nil
	})
}
func (mns *memoryNodeStore) Close(ctx context.Context) error {
	return nil
}

func (mns *memoryNodeStore) generateRandomNodeID(parentID string) string {
	mns.nextID++
	return fmt.Sprintf("%d", mns.nextID)
	//return fmt.Sprintf("%016x-%016x", uint64(time.Now().UnixNano()), rand.Uint64())
}

func (mns *memoryNodeStore) withLock(ctx context.Context, f func(ctx context.Context) error) error {
	gotLock := make(chan struct{})
	go func() {
		mns.lock.Lock()
		select {
		case <-ctx.Done():
			mns.lock.Unlock()
		case gotLock <- struct{}{}:
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gotLock:
		defer mns.lock.Unlock()
	}
	return f(ctx)
}

func (mns *memoryNodeStore) withRLock(ctx context.Context, f func(ctx context.Context) error) error {
	gotLock := make(chan struct{})
	go func() {
		mns.lock.RLock()
		select {
		case <-ctx.Done():
			mns.lock.RUnlock()
		case gotLock <- struct{}{}:
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gotLock:
		defer mns.lock.RUnlock()
	}
	return f(ctx)
}
