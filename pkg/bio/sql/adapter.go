package sql

import "context"

/*
Adapter is an interface providing the methods
needed to implement a Set with a database backend.

ColumnName takes a string feature name and returns
a column name for the feature in a string or an error

CreateDiscreteValuesTable should create a table containing
the different values discrete features can take in the
samples of the working sets.

CreateSampleTable should create a table for the samples,
using foreign keys to the discrete value table for discrete
features and a suitable float64 representation for continuous
ones. It should also generate an id column.

AddDiscreteValues should add to the discrete value table the
given discrete values, and return an error if any cannot be added.

ListDiscreteValues should return a map of integer to string that
relates numeric ids of the discrete values to their string values,
or an error.

AddSamples should add a sample to the samples table for each
rawSample received. A rawSample here is a map of column name to an
interface containing the numeric id for a discrete feature value
or a float64 for a continuous feature value. Samples should be
added considering all discrete and continuous feature columns only.
NULL values should be used for column values not available in the
rawSample. The number of samples added or an error must be returned.

ListSamples should provide a slice of rawSamples as described above
satisfying the given feature criteria and specifying the values for
the given discrete and continuous feature columns, or an error.

IterateOnSamples is similar to ListSamples, but takes an additional
lambda to iterate on the samples rather than returned them all. This
method should call the lambda for every sample satisfying the criteria.
The lambda takes an index for the sample (0,1,2,...) and a raw sample
and returns a boolean and an error, which must be true and nil in order
for this method not to stop. This method should return an error
if the samples cannot be traversed or any error the lambda returns.

CountSamples should return the number of samples in the samples table
that satisfy the riven feature criteria or an error if they cannot be
counted.

ListSampleDiscreteFeatureValues takes a discrete feature column name and
a slice of feature criteria and should return an slice with the numeric
IDs for the different values for the given feature column name on
samples satisfying the given criteria, or an error.

ListSampleContinuousFeatureValues takes a continuous feature column name
and a slice of feature criteria and should return an slice with the
different values for the given feature column name on samples satisfying
the given criteria, or an error.

CountSampleDiscreteFeatureValues takes a discrete feature column name
and a slice of feature criteria and should return a map relating the
numeric IDs for the discrete values for the given feature column on
samples in the table satisfying the given criteria to the number of
times they appear among the samples satisfying the given criteria or
an error.

CountSampleContinuousFeatureValues takes a continuous feature column
name and a slice of feature criteria and should return a map relating
the continuous values for the given column name on samples in the
table satisfying the given criteria to the number of times they
appear among the samples satisfying the given criteria or an error.
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
