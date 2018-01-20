package json

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pbanos/botanic/feature"

	"github.com/pbanos/botanic/tree"
)

func WriteJSONTree(ctx context.Context, t *tree.Tree, w io.Writer) error {
	err := marshalJSONTreeHeader(ctx, t, w)
	if err != nil {
		return err
	}
	var i int
	err = t.Traverse(ctx, false, func(ctx context.Context, n *tree.Node) error {
		err := writeNode(ctx, i, n, w)
		i++
		return err
	})
	if err != nil {
		return err
	}
	return marshalJSONTreeFooter(ctx, t, w)
}

func ReadJSONTree(ctx context.Context, t *tree.Tree, features []feature.Feature, r io.Reader) error {
	dec := json.NewDecoder(r)
	jt := &struct {
		RootID       string             `json:"rootID"`
		ClassFeature string             `json:"classFeature"`
		Nodes        []*json.RawMessage `json:"nodes"`
	}{}
	err := dec.Decode(jt)
	if err != nil {
		return err
	}
	var cf feature.Feature
	for _, f := range features {
		if f.Name() == jt.ClassFeature {
			cf = f
			break
		}
	}
	if cf == nil {
		return fmt.Errorf("no class feature defined")
	}
	if jt.RootID == "" {
		return fmt.Errorf("no root node id available")
	}
	t.ClassFeature = cf
	t.RootID = jt.RootID
	for _, jn := range jt.Nodes {
		n := &tree.Node{}
		err = UnmarshalJSONNodeWithFeatures(n, *jn, features)
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
	jFeatureName, err := json.Marshal(t.ClassFeature.Name())
	if err != nil {
		return err
	}
	header := fmt.Sprintf(`{"rootID":%s,"classFeature":%s,"nodes":[`, jrootID, jFeatureName)
	_, err = w.Write([]byte(header))
	return err
}

func writeNode(ctx context.Context, i int, n *tree.Node, w io.Writer) error {
	if i != 0 {
		_, err := w.Write([]byte(","))
		if err != nil {
			return err
		}
	}
	jn, err := MarshalJSONNode(n)
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
