package main

import (
	"fmt"
	"os"

	"github.com/pbanos/botanic/pkg/bio"
	"github.com/pbanos/botanic/pkg/botanic"
	"github.com/spf13/cobra"
)

type predictCmdConfig struct {
	treeInput      string
	metadataInput  string
	classFeature   string
	undefinedValue string
}

type stdoutFeatureValueRequester string

func predictCmd(rootConfig *rootCmdConfig) *cobra.Command {
	config := &predictCmdConfig{}
	cmd := &cobra.Command{
		Use:   "predict",
		Short: "Predict a value for a sample answering questions",
		Long:  `Use the loaded tree to predict the class feature value for a sample answering a reduced set of question about its features`,
		Run: func(cmd *cobra.Command, args []string) {
			err := config.Validate()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			features, err := bio.ReadYMLFeaturesFromFile(config.metadataInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			tree, err := loadTree(config.treeInput)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			prediction, err := predict(tree, features, config.undefinedValue)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(4)
			}
			fmt.Printf("Predicted values along their probabilities are %v\n", prediction)
		},
	}
	cmd.PersistentFlags().StringVarP(&(config.metadataInput), "metadata", "m", "", "path to a YML file with metadata describing the different features available available on the input file (required)")
	cmd.PersistentFlags().StringVarP(&(config.treeInput), "tree", "t", "", "path to a file from which the tree to test will be read and parsed as JSON (required)")
	cmd.PersistentFlags().StringVarP(&(config.classFeature), "class-feature", "c", "", "name of the feature the generated tree should predict (required)")
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
	if pcc.classFeature == "" {
		return fmt.Errorf("required class-feature flag was not set")
	}
	return nil
}

func predict(tree *botanic.Tree, features []botanic.Feature, undefinedValue string) (*botanic.Prediction, error) {
	sample := bio.NewReadSample(os.Stdin, features, stdoutFeatureValueRequester(undefinedValue), undefinedValue)
	return tree.Predict(sample)
}

func (sfvr stdoutFeatureValueRequester) RequestValueFor(f botanic.Feature) error {
	switch f := f.(type) {
	case *botanic.DiscreteFeature:
		fmt.Printf("Please provide the sample's %s:\n(valid values are %v or %s if undefined)\n", f.Name(), f.AvailableValues(), string(sfvr))
	case *botanic.ContinuousFeature:
		fmt.Printf("Please provide the sample's %s:\n(valid values are real numbers or %s if undefined)\n", f.Name(), string(sfvr))
	default:
		return fmt.Errorf("unknown feature type %T", f)
	}
	return nil
}

func (sfvr stdoutFeatureValueRequester) RejectValueFor(f botanic.Feature, value interface{}) error {
	switch f := f.(type) {
	case *botanic.DiscreteFeature:
		fmt.Printf("%v is not a valid value for the sample's %s. Please provide one of %v or %s if undefined.\n", value, f.Name(), f.AvailableValues(), string(sfvr))
	case *botanic.ContinuousFeature:
		fmt.Printf("%v is not a valid value for the sample's %s. Please provide a real number or %s if undefined.\n", value, f.Name(), string(sfvr))
	default:
		return fmt.Errorf("unknown feature type %T", f)
	}
	return nil
}
