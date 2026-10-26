// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// Null equality matches null and missing values, which an index holds as null
// entries; the results must match a collection scan's.

func nullEqSetup(values []interface{}, keys bson.D, opts *options.IndexOptions) func(context.Context, *mongo.Collection) error {
	return func(ctx context.Context, col *mongo.Collection) error {
		docs := make([]interface{}, 0, len(values)*4+4)
		for i := 0; i < len(values)*4; i++ {
			docs = append(docs, bson.D{{Key: "_id", Value: int32(i)}, {Key: "n", Value: values[i%len(values)]}, {Key: "a", Value: bson.D{{Key: "n", Value: values[i%len(values)]}}}})
		}
		for i := 0; i < 4; i++ {
			docs = append(docs, bson.D{{Key: "_id", Value: int32(1000 + i)}})
		}
		if _, err := col.InsertMany(ctx, docs); err != nil {
			return err
		}
		_, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys, Options: opts})
		return err
	}
}

func TestIndex_NullEq(t *testing.T) {
	scalars := []interface{}{nil, int32(1), "x", bson.D{{Key: "x", Value: nil}}, false, 0.0}
	withArrays := append(append([]interface{}{}, scalars...), bson.A{nil, int32(2)}, bson.A{int32(3)}, bson.A{})
	nKey := bson.D{{Key: "n", Value: 1}}
	for _, tc := range []struct {
		name   string
		values []interface{}
		keys   bson.D
		opts   *options.IndexOptions
		filter bson.D
	}{
		{"Bare", scalars, nKey, options.Index(), bson.D{{Key: "n", Value: nil}}},
		{"Eq", scalars, nKey, options.Index(), bson.D{{Key: "n", Value: bson.D{{Key: "$eq", Value: nil}}}}},
		{"Sparse", scalars, nKey, options.Index().SetSparse(true), bson.D{{Key: "n", Value: nil}}},
		{"Multikey", withArrays, nKey, options.Index(), bson.D{{Key: "n", Value: nil}}},
		{"Compound", scalars, bson.D{{Key: "n", Value: 1}, {Key: "_id", Value: 1}}, options.Index(), bson.D{{Key: "n", Value: nil}}},
		{"Dotted", scalars, bson.D{{Key: "a.n", Value: 1}}, options.Index(), bson.D{{Key: "a.n", Value: nil}}},
		{"WithOther", scalars, nKey, options.Index(), bson.D{{Key: "n", Value: nil}, {Key: "_id", Value: bson.D{{Key: "$gte", Value: int32(10)}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Index_NullEq_" + tc.name,
				Support: harness.DumboDBFull,
				Setup:   nullEqSetup(tc.values, tc.keys, tc.opts),
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetProjection(bson.D{{Key: "_id", Value: 1}})
					cursor, err := col.Find(ctx, tc.filter, opts)
					if err != nil {
						return nil, err
					}
					var results []bson.D
					return results, cursor.All(ctx, &results)
				},
			})
		})
	}
}
