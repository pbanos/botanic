/*
Package mongodataset provides a implementation of dataset.Dataset
that uses a MongoDB database as backend.
*/
package mongodataset

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/pbanos/botanic/dataset"
	"github.com/pbanos/botanic/feature"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
Dataset is a dataset.Dataset to which samples can be added
and from which samples can be sequentially read
*/
type Dataset interface {
	dataset.Dataset
	Write(context.Context, []dataset.Sample) (int, error)
	Read(context.Context) (<-chan dataset.Sample, <-chan error)
}

type mongodataset struct {
	session    *mgo.Session
	features   []feature.Feature
	criteria   []feature.Criterion
	mongoQuery bson.M
	entropy    *float64
}

const (
	samplesCollectionName = "samples"
)

/*
Open takes a MongoDB database session and returns a
dataset.Dataset that works on the default database for
that session or an error if it fails to connect to it.
*/
func Open(ctx context.Context, session *mgo.Session, features []feature.Feature) (Dataset, error) {
	mds := &mongodataset{session, features, nil, nil, nil}
	err := mds.ensureIndexes()
	if err != nil {
		return nil, err
	}
	return mds, nil
}

func (mds *mongodataset) Entropy(ctx context.Context, f feature.Feature) (float64, error) {
	if mds.entropy != nil {
		return *mds.entropy, nil
	}
	var result, count float64
	featureValueCounts, err := mds.CountFeatureValues(ctx, f)
	if err != nil {
		return 0.0, err
	}
	for _, c := range featureValueCounts {
		count += float64(c)
	}
	for _, c := range featureValueCounts {
		probValue := float64(c) / count
		result -= probValue * math.Log(probValue)
	}
	mds.entropy = &result
	return result, nil
}

func (mds *mongodataset) SubsetWith(ctx context.Context, fc feature.Criterion) (dataset.Dataset, error) {
	return &mongodataset{mds.session, mds.features, append([]feature.Criterion{fc}, mds.criteria...), nil, nil}, nil
}
func (mds *mongodataset) FeatureValues(ctx context.Context, f feature.Feature) ([]interface{}, error) {
	if mds.mongoQuery == nil {
		mds.query()
	}
	iter := mds.samplesCollection().Pipe([]bson.M{{"$match": mds.mongoQuery}, {"$group": bson.M{"_id": fmt.Sprintf("$%s", f.Name())}}}).Iter()
	defer iter.Close()
	var doc bson.M
	var result []interface{}
	for iter.Next(&doc) {
		result = append(result, doc["_id"])
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func (mds *mongodataset) CountFeatureValues(ctx context.Context, f feature.Feature) (map[string]int, error) {
	if mds.mongoQuery == nil {
		mds.query()
	}
	iter := mds.samplesCollection().Pipe([]bson.M{{"$match": mds.mongoQuery}, {"$group": bson.M{"_id": fmt.Sprintf("$%s", f.Name()), "count": bson.M{"$sum": 1}}}}).Iter()
	defer iter.Close()
	var doc bson.M
	result := make(map[string]int)
	for iter.Next(&doc) {
		count, ok := doc["count"].(int)
		if !ok {
			return nil, fmt.Errorf("counting feature values: mongo aggregation query returned a %T instead of an int as count", doc["count"])
		}
		result[fmt.Sprintf("%v", doc["_id"])] = count
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func (mds *mongodataset) Samples(ctx context.Context) ([]dataset.Sample, error) {
	var samples []dataset.Sample
	count, err := mds.Count(ctx)
	if err == nil {
		samples = make([]dataset.Sample, 0, count)
	}
	sampleChan, errs := mds.Read(ctx)
	for sample := range sampleChan {
		samples = append(samples, sample)
	}
	err = <-errs
	return samples, err
}

func (mds *mongodataset) Count(context.Context) (int, error) {
	return mds.query().Count()
}

func (mds *mongodataset) Criteria(context.Context) ([]feature.Criterion, error) {
	return mds.criteria, nil
}

func (mds *mongodataset) Write(ctx context.Context, samples []dataset.Sample) (int, error) {
	docs := make([]interface{}, 0, len(samples))
	for _, s := range samples {
		doc := make(bson.M)
		for _, f := range mds.features {
			value, err := s.ValueFor(ctx, f)
			if err != nil {
				return 0, err
			}
			if value != nil {
				doc[f.Name()] = value
			}
		}
		docs = append(docs, doc)
	}
	err := mds.samplesCollection().Insert(docs...)
	if err != nil {
		return 0, err
	}
	return len(samples), nil
}
func (mds *mongodataset) Read(ctx context.Context) (<-chan dataset.Sample, <-chan error) {
	samples := make(chan dataset.Sample)
	errs := make(chan error, 1)
	go func() {
		var doc bson.M
		var err error
		iter := mds.query().Iter()
		defer iter.Close()
		for iter.Next(&doc) {
			s := dataset.NewSample(doc)
			select {
			case <-ctx.Done():
				err = ctx.Err()
				break
			case samples <- s:
			}
		}
		if err == nil {
			err = iter.Err()
		}
		if err != nil {
			errs <- err
		}
		close(errs)
		close(samples)
	}()
	return samples, errs
}

func (mds *mongodataset) ensureIndexes() error {
	for _, f := range mds.features {
		fName := f.Name()
		if fName == "_id" {
			return fmt.Errorf("invalid feature name %q: reserved collection field", "_id")
		}
		if strings.ContainsAny(fName, ".$") {
			return fmt.Errorf("invalid feature name %q: contains reserved characters %q or %q", fName, ".", "$")
		}
		index := mgo.Index{
			Key:        []string{fName},
			Background: true,
			Sparse:     true,
		}
		err := mds.samplesCollection().EnsureIndex(index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mds *mongodataset) samplesCollection() *mgo.Collection {
	return mds.session.DB("").C(samplesCollectionName)
}

func (mds *mongodataset) query() *mgo.Query {
	if mds.mongoQuery == nil {
		mds.mongoQuery = make(bson.M)
		for _, fc := range mds.criteria {
			fName := fc.Feature().Name()
			switch qfc := fc.(type) {
			case feature.DiscreteCriterion:
				mds.mongoQuery[fName] = qfc.Value()
			case feature.ContinuousCriterion:
				a, b := qfc.Interval()
				var rangeValue bson.M
				if v, ok := mds.mongoQuery[fName]; ok && v != nil {
					rangeValue = v.(bson.M)
				}
				if rangeValue == nil {
					rangeValue = make(bson.M)
				}
				if !math.IsInf(a, 0) {
					v, ok := rangeValue["$gte"].(float64)
					if !ok || v < a {
						rangeValue["$gte"] = a
					}
				}
				if !math.IsInf(b, 0) {
					v, ok := rangeValue["$lt"].(float64)
					if !ok || v > b {
						rangeValue["$lt"] = b
					}
				}
				mds.mongoQuery[fName] = rangeValue
			}
		}
	}
	return mds.samplesCollection().Find(mds.mongoQuery)
}
