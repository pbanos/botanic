package bio

import (
	"fmt"
	"io/ioutil"

	"github.com/pbanos/botanic/pkg/botanic"
	yaml "gopkg.in/yaml.v2"
)

/*
ReadYMLFeatures takes a slice of bytes with a feature specification in YML and
returns a slice of features parsed from it or an error.
The YML is expected to be an object containing a features property. The value for this
should be an object with a property for each feature with its name and either a
string value of 'continuous' for continuous features or a list of valid values
for discrete features.
*/
func ReadYMLFeatures(md []byte) ([]botanic.Feature, error) {
	metadata := struct {
		Features map[string]interface{}
	}{}
	err := yaml.Unmarshal(md, &metadata)
	if err != nil {
		return nil, fmt.Errorf("parsing yml features: %v", err)
	}
	if metadata.Features == nil {
		return nil, fmt.Errorf("metadata file has no feature information")
	}
	features := []botanic.Feature{}
	for fn, vs := range metadata.Features {
		switch values := vs.(type) {
		case string:
			features = append(features, botanic.NewContinuousFeature(fn))
		case []interface{}:
			stringVs := []string{}
			for _, v := range values {
				stringVs = append(stringVs, fmt.Sprintf("%v", v))
			}
			features = append(features, botanic.NewDiscreteFeature(fn, stringVs))
		case []string:
			features = append(features, botanic.NewDiscreteFeature(fn, values))
		default:
			return nil, fmt.Errorf("invalid feature declaration of type %T\n", vs)
		}
	}
	return features, nil
}

/*
ReadYMLFeaturesFromFile takes a filepath string, reads its contents and uses
ReadYMLFeatures to parse it and return a slice of parsed features or an error.
If the file indicated by the filepath cannot be opened for reading an error
will be returned.
*/
func ReadYMLFeaturesFromFile(filepath string) ([]botanic.Feature, error) {
	md, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("reading features yml file %s: %v", filepath, err)
	}
	features, err := ReadYMLFeatures(md)
	if err != nil {
		err = fmt.Errorf("parsing features yml file %s: %v", filepath, err)
	}
	return features, err
}
