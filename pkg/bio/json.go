package bio

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/pbanos/botanic/pkg/botanic"
)

/*
WriteJSONTree takes an io.Writer and a botanic.Tree and prints
a JSON representation of the tree onto the writer. It returns
an error if serialization or printing fails, nil otherwise.
*/
func WriteJSONTree(w io.Writer, tree *botanic.Tree) error {
	encoder := json.NewEncoder(w)
	err := encoder.Encode(tree)
	if err != nil {
		return fmt.Errorf("serializing tree as JSON: %v", err)
	}
	return nil
}

/*
WriteJSONTreeToFile takes a filepath string and a botanic.Tree
and tries to create a file on the given filepath and later use
WriteJSONTree to write a JSON representation of the tree on it.
It returns an error if the file cannot be opened for writing or
serialization or printing fails, nil otherwise.
*/
func WriteJSONTreeToFile(filepath string, tree *botanic.Tree) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()
	return WriteJSONTree(f, tree)
}

/*
ReadJSONTree takes an io.Reader and attempts to JSON-decode a
tree from it. It returns the read tree or an error.
*/
func ReadJSONTree(r io.Reader) (*botanic.Tree, error) {
	decoder := json.NewDecoder(r)
	tree := &botanic.Tree{}
	err := decoder.Decode(tree)
	if err != nil {
		return nil, fmt.Errorf("decoding json Tree: %v", err)
	}
	return tree, nil
}
