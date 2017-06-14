package sql

/*
Adapter is an interface providing the methods
needed to implement a Set with a database backend.
*/
type Adapter interface {
	ColumnName(string) (string, error)

	CreateDiscreteValuesTable() error
	CreateSampleTable(discreteFeatureColumns, continuousFeatureColumns []string) error

	AddDiscreteValues([]string) (int, error)
	ListDiscreteValues() (map[int]string, error)

	AddSamples(rawSamples []map[string]interface{}, discreteFeatureColumns, continuousFeatureColumns []string) (int, error)
	ListSamples(criteria []*FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string) ([]map[string]interface{}, error)
	IterateOnSamples(criteria []*FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string, lambda func(int, map[string]interface{}) (bool, error)) error
	CountSamples([]*FeatureCriterion) (int, error)

	ListSampleDiscreteFeatureValues(string, []*FeatureCriterion) ([]int, error)
	ListSampleContinuousFeatureValues(string, []*FeatureCriterion) ([]float64, error)
	CountSampleDiscreteFeatureValues(string, []*FeatureCriterion) (map[int]int, error)
	CountSampleContinuousFeatureValues(string, []*FeatureCriterion) (map[float64]int, error)
}
