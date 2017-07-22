package sql

import "context"

/*
Adapter is an interface providing the methods
needed to implement a Set with a database backend.
*/
type Adapter interface {
	ColumnName(string) (string, error)

	CreateDiscreteValuesTable(ctx context.Context) error
	CreateSampleTable(ctx context.Context, discreteFeatureColumns, continuousFeatureColumns []string) error

	AddDiscreteValues(context.Context, []string) (int, error)
	ListDiscreteValues(ctx context.Context) (map[int]string, error)

	AddSamples(ctx context.Context, rawSamples []map[string]interface{}, discreteFeatureColumns, continuousFeatureColumns []string) (int, error)
	ListSamples(ctx context.Context, criteria []*FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string) ([]map[string]interface{}, error)
	IterateOnSamples(ctx context.Context, criteria []*FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string, lambda func(int, map[string]interface{}) (bool, error)) error
	CountSamples(context.Context, []*FeatureCriterion) (int, error)

	ListSampleDiscreteFeatureValues(context.Context, string, []*FeatureCriterion) ([]int, error)
	ListSampleContinuousFeatureValues(context.Context, string, []*FeatureCriterion) ([]float64, error)
	CountSampleDiscreteFeatureValues(context.Context, string, []*FeatureCriterion) (map[int]int, error)
	CountSampleContinuousFeatureValues(context.Context, string, []*FeatureCriterion) (map[float64]int, error)
}
