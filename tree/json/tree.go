package json

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pbanos/botanic/feature"

	"github.com/pbanos/botanic/tree"
)

/*
WriteJSONTree takes a context.Context, a pointer to a tree.Tree
a NodeEncodeDecoder and an io.Writer and serializes the given tree
as JSON onto the io.Writer.
A tree is serialized as a JSON object with the following fields:
* "rootID": a string with the ID of the node at the root of the tree
* "label": a string with the name of the feature the tree predicts
* "nodes": an array containing the nodes that can be traversed on the tree
  serialized by the given NodeEncodeDecoder.
An error is returned if the tree cannot be traversed, serialized or written
onto the io.Writer.
*/
func WriteJSONTree(ctx context.Context, t *tree.Tree, ned NodeEncodeDecoder, w io.Writer) error {
	err := marshalJSONTreeHeader(ctx, t, w)
	if err != nil {
		return err
	}
	var i int
	err = t.Traverse(ctx, false, func(ctx context.Context, n *tree.Node) error {
		err := writeNode(ctx, i, n, ned, w)
		i++
		return err
	})
	if err != nil {
		return err
	}
	return marshalJSONTreeFooter(ctx, t, w)
}

/*
ReadJSONTree takes a context.Context, a pointer to a tree.Tree and an
io.Reader and unmarshals the contents of the io.Reader onto the given
tree.
A tree is expected to be a JSON object with the following fields:
* "rootID": a string with the ID of the node at the root of the tree
* "label": a string with the name of the feature the tree predicts
* "nodes": an array containing the nodes that can be traversed on the tree
  unmarshalled by UnmarshalJSONNodeWithFeatures.
An error is returned if the JSON cannot be read from the io.Reader or
unmarshalled onto the tree.
*/
func ReadJSONTree(ctx context.Context, t *tree.Tree, ned NodeEncodeDecoder, features []feature.Feature, r io.Reader) error {
	dec := json.NewDecoder(r)
	jt := &struct {
		RootID string             `json:"rootID"`
		Label  string             `json:"label"`
		Nodes  []*json.RawMessage `json:"nodes"`
	}{}
	err := dec.Decode(jt)
	if err != nil {
		return err
	}
	var cf feature.Feature
	for _, f := range features {
		if f.Name() == jt.Label {
			cf = f
			break
		}
	}
	if cf == nil {
		return fmt.Errorf("no label feature defined")
	}
	if jt.RootID == "" {
		return fmt.Errorf("no root node id available")
	}
	t.Label = cf
	t.RootID = jt.RootID
	for _, jn := range jt.Nodes {
		n, err := ned.Decode(*jn)
		if err != nil {
			return err
		}
		err = t.NodeStore.Store(ctx, n)
		if err != nil {
			return err
		}
	}
	return nil
}

func marshalJSONTreeHeader(ctx context.Context, t *tree.Tree, w io.Writer) error {
	jrootID, err := json.Marshal(t.RootID)
	if err != nil {
		return err
	}
	jFeatureName, err := json.Marshal(t.Label.Name())
	if err != nil {
		return err
	}
	header := fmt.Sprintf(`{"rootID":%s,"label":%s,"nodes":[`, jrootID, jFeatureName)
	_, err = w.Write([]byte(header))
	return err
}

func writeNode(ctx context.Context, i int, n *tree.Node, ned NodeEncodeDecoder, w io.Writer) error {
	if i != 0 {
		_, err := w.Write([]byte(","))
		if err != nil {
			return err
		}
	}
	jn, err := ned.Encode(n)
	if err != nil {
		return err
	}
	_, err = w.Write(jn)
	return err
}

func marshalJSONTreeFooter(ctx context.Context, t *tree.Tree, w io.Writer) error {
	_, err := w.Write([]byte(`]}`))
	return err
}
