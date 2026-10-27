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

// A sparse index holds the documents in which its field exists: $exists on
// that field, and any query hinted onto the index, read only its members.

func sparseMembersSetup(keys bson.D) func(context.Context, *mongo.Collection) error {
	return func(ctx context.Context, col *mongo.Collection) error {
		values := []interface{}{int32(1), nil, bson.A{}, bson.A{int32(2), int32(3)}, "x", bson.D{{Key: "y", Value: int32(1)}}}
		docs := make([]interface{}, 0, 60)
		for i := 0; i < 60; i++ {
			d := bson.D{{Key: "_id", Value: int32(i)}, {Key: "pad", Value: int32(i)}}
			if i%3 == 0 {
				d = append(d, bson.E{Key: "sp", Value: values[(i/3)%len(values)]})
			}
			if i%4 == 0 {
				d = append(d, bson.E{Key: "other", Value: int32(i)})
			}
			docs = append(docs, d)
		}
		if _, err := col.InsertMany(ctx, docs); err != nil {
			return err
		}
		_, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys, Options: options.Index().SetSparse(true).SetName("idx")})
		return err
	}
}

func TestIndex_SparseMembers(t *testing.T) {
	single := bson.D{{Key: "sp", Value: 1}}
	compound := bson.D{{Key: "sp", Value: 1}, {Key: "other", Value: 1}}
	exists := func(v interface{}) bson.D { return bson.D{{Key: "sp", Value: bson.D{{Key: "$exists", Value: v}}}} }
	for _, tc := range []struct {
		name   string
		keys   bson.D
		filter bson.D
		hint   bool
	}{
		{"Exists", single, exists(true), false},
		{"ExistsOne", single, exists(int32(1)), false},
		{"ExistsFalse", single, exists(false), false},
		{"ExistsCompound", compound, exists(true), false},
		{"HintUnindexedFilter", single, bson.D{{Key: "pad", Value: bson.D{{Key: "$gte", Value: int32(10)}}}}, true},
		{"HintEmptyFilter", single, bson.D{}, true},
		{"HintCompound", compound, bson.D{{Key: "pad", Value: bson.D{{Key: "$exists", Value: true}}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Index_SparseMembers_" + tc.name,
				Support: harness.DumboDBFull,
				Setup:   sparseMembersSetup(tc.keys),
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetProjection(bson.D{{Key: "_id", Value: 1}})
					if tc.hint {
						opts.SetHint("idx")
					}
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
