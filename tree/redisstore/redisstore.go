package redisstore

import (
	"context"
	"fmt"

	"github.com/pbanos/botanic/tree"
	"gopkg.in/redis.v5"
)

/*
NodeEncodeDecoder is an interface for objects
that allow encoding nodes into slices of
bytes and decoding them back to nodes.
*/
type NodeEncodeDecoder interface {

	//Encode receives a *tree.Node
	// and returns a slice of bytes with the dataset
	//encoded or an error if the encoding could not
	//be performed for some reason.
	Encode(*tree.Node) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a *tree.Node decoded from the
	//slice of bytes or an error if the decoding
	//could not be performed for some reason.
	Decode([]byte) (*tree.Node, error)
}

type redisStore struct {
	rc      *redis.Client
	prefix  string
	nencdec NodeEncodeDecoder
}

//New builds a tree.NodeStore backed by a redis DB
func New(rc *redis.Client, prefix string, nencdec NodeEncodeDecoder) tree.NodeStore {
	return &redisStore{rc, prefix, nencdec}
}

func (rs *redisStore) Create(ctx context.Context, n *tree.Node) error {
	var ok bool
	for !ok {
		n.ID = randString(20)
		data, err := rs.nencdec.Encode(n)
		if err != nil {
			return fmt.Errorf("creating node: encoding node: %v", err)
		}
		ok, err = rs.rc.SetNX(rs.keyFor(n.ID), data, 0).Result()
		if err != nil {
			return fmt.Errorf("creating node in redis: %v", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}
func (rs *redisStore) Get(ctx context.Context, id string) (*tree.Node, error) {
	data, err := rs.rc.Get(rs.keyFor(id)).Result()
	if err != nil {
		return nil, fmt.Errorf("retrieving node %q: %v", id, err)
	}
	if data == "" {
		return nil, nil
	}
	n, err := rs.nencdec.Decode([]byte(data))
	if err != nil {
		return nil, fmt.Errorf("retrieving node %q: decoding %q: %v", id, data, err)
	}
	return n, nil
}
func (rs *redisStore) Store(ctx context.Context, n *tree.Node) error {
	redisID := rs.keyFor(n.ID)
	data, err := rs.nencdec.Encode(n)
	if err != nil {
		return fmt.Errorf("storing node %q: encoding node: %v", redisID, err)
	}
	_, err = rs.rc.Set(redisID, data, 0).Result()
	if err != nil {
		return fmt.Errorf("storing node %q in redis: %v", redisID, err)
	}
	return nil
}
func (rs *redisStore) Delete(ctx context.Context, n *tree.Node) error {
	redisID := rs.keyFor(n.ID)
	_, err := rs.rc.Del(redisID).Result()
	if err != nil {
		return fmt.Errorf("deleting node %q from redis: %v", redisID, err)
	}
	return nil
}
func (rs *redisStore) Close(ctx context.Context) error {
	return nil
}

func (rs *redisStore) keyFor(id string) string {
	return fmt.Sprintf("%s:%s", rs.prefix, id)
}
