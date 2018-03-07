package main

import (
	"context"
	"fmt"
	"os"

	"github.com/pbanos/botanic/dataset/inputsample"
	"github.com/pbanos/botanic/feature"
	"github.com/pbanos/botanic/feature/yaml"
	"github.com/pbanos/botanic/tree"
	"github.com/spf13/cobra"
)

type predictCmdConfig struct {
	*treeCmdConfig
	undefinedValue string
}

type stdoutFeatureValueRequester string

func predictCmd(treeConfig *treeCmdConfig) *cobra.Command {
	config := &predictCmdConfig{treeCmdConfig: treeConfig}
	cmd := &cobra.Command{
		Use:   "predict",
		Short: "Predict a value for a sample answering questions",
		Long:  `Use the loaded tree to predict the label feature value for a sample answering a reduced set of question about its features`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			features, err := yaml.ReadFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			tree, err := loadTree(context.Background(), config.treeInput, features)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			prediction, err := predict(context.Background(), tree, features, config.undefinedValue)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			fmt.Printf("Predicted values along their probabilities are %v\n", prediction)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.treeInput), "tree", "t", "", "path to a file from which the tree to test will be read and parsed as JSON (required)")
	cmd.PersistentFlags().StringVarP(&(config.undefinedValue), "undefined-value", "u", "?", "value to input to define a sample's value for a feature as undefined")
	return cmd
}

func (pcc *predictCmdConfig) Validate() error {
	if pcc.metadataInput == "" {
		return fmt.Errorf("required metadata flag was not set")
	}
	if pcc.treeInput == "" {
		return fmt.Errorf("required tree flag was not set")
	}
	return nil
}

func predict(ctx context.Context, tree *tree.Tree, features []feature.Feature, undefinedValue string) (*tree.Prediction, error) {
	sample := inputsample.New(os.Stdin, features, stdoutFeatureValueRequester(undefinedValue), undefinedValue)
	return tree.Predict(ctx, sample)
}

func (sfvr stdoutFeatureValueRequester) RequestValueFor(f feature.Feature) error {
	switch f := f.(type) {
	case *feature.DiscreteFeature:
		fmt.Printf("Please provide the sample's %s:\n(valid values are %v or %s if undefined)\n", f.Name(), f.AvailableValues(), string(sfvr))
	case *feature.ContinuousFeature:
		fmt.Printf("Please provide the sample's %s:\n(valid values are real numbers or %s if undefined)\n", f.Name(), string(sfvr))
	default:
		return fmt.Errorf("unknown feature type %T", f)
	}
	return nil
}

func (sfvr stdoutFeatureValueRequester) RejectValueFor(f feature.Feature, value interface{}) error {
	switch f := f.(type) {
	case *feature.DiscreteFeature:
		fmt.Printf("%v is not a valid value for the sample's %s. Please provide one of %v or %s if undefined.\n", value, f.Name(), f.AvailableValues(), string(sfvr))
	case *feature.ContinuousFeature:
		fmt.Printf("%v is not a valid value for the sample's %s. Please provide a real number or %s if undefined.\n", value, f.Name(), string(sfvr))
	default:
		return fmt.Errorf("unknown feature type %T", f)
	}
	return nil
}
