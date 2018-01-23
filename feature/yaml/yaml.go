/*
Package yaml provides methods to parse feature.Feature specifications
also known as metadata, from YAML documents.
*/
package yaml

import (
	"fmt"
	"io/ioutil"

	"github.com/pbanos/botanic/feature"
	yaml "gopkg.in/yaml.v2"
)

/*
ReadFeatures takes a slice of bytes with a feature specification in YML and
returns a slice of features parsed from it or an error.
The YML is expected to be an object containing a features property. The value for this
should be an object with a property for each feature with its name and either a
string value of 'continuous' for continuous features or a list of valid values
for discrete features.
*/
func ReadFeatures(md []byte) ([]feature.Feature, error) {
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
	features := []feature.Feature{}
	for fn, vs := range metadata.Features {
		switch values := vs.(type) {
		case string:
			features = append(features, feature.NewContinuousFeature(fn))
		case []interface{}:
			stringVs := []string{}
			for _, v := range values {
				stringVs = append(stringVs, fmt.Sprintf("%v", v))
			}
			features = append(features, feature.NewDiscreteFeature(fn, stringVs))
		case []string:
			features = append(features, feature.NewDiscreteFeature(fn, values))
		default:
			return nil, fmt.Errorf("invalid feature declaration of type %T", vs)
		}
	}
	return features, nil
}

/*
ReadFeaturesFromFile takes a filepath string, reads its contents and uses
ReadFeatures to parse it and return a slice of parsed features or an error.
If the file indicated by the filepath cannot be opened for reading an error
will be returned.
*/
func ReadFeaturesFromFile(filepath string) ([]feature.Feature, error) {
	md, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("reading features yml file %s: %v", filepath, err)
	}
	features, err := ReadFeatures(md)
	if err != nil {
		err = fmt.Errorf("parsing features yml file %s: %v", filepath, err)
	}
	return features, err
}
