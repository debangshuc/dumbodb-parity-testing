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

// A partial index whose condition is a range serves a query whose own
// condition on that field lies within the range; the results must match a
// collection scan's.

func partialRangeSetup(ctx context.Context, col *mongo.Collection) error {
	docs := make([]interface{}, 0, 220)
	for i := 0; i < 200; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: int32(i)}, {Key: "k", Value: int32(i % 20)}, {Key: "grp", Value: int32(i)}})
	}
	extra := []interface{}{"50", 99.5, int64(100), bson.A{int32(5), int32(500)}, bson.A{int32(500)}, nil}
	for i, v := range extra {
		docs = append(docs, bson.D{{Key: "_id", Value: int32(200 + i)}, {Key: "k", Value: int32(5)}, {Key: "grp", Value: v}})
	}
	docs = append(docs, bson.D{{Key: "_id", Value: int32(300)}, {Key: "k", Value: int32(5)}})
	if _, err := col.InsertMany(ctx, docs); err != nil {
		return err
	}
	return createPartialIndex(ctx, col, bson.D{{Key: "k", Value: 1}}, bson.D{{Key: "grp", Value: bson.D{{Key: "$lt", Value: int32(100)}}}}, "k_partial")
}

func TestIndex_PartialRange(t *testing.T) {
	rng := func(ops ...interface{}) bson.D {
		d := bson.D{}
		for i := 0; i < len(ops); i += 2 {
			d = append(d, bson.E{Key: ops[i].(string), Value: ops[i+1]})
		}
		return d
	}
	for _, tc := range []struct {
		name string
		grp  interface{}
	}{
		{"SameRange", rng("$lt", int32(100))},
		{"Tighter", rng("$lt", int32(50))},
		{"Between", rng("$gte", int32(3), "$lt", int32(60))},
		{"Equality", int32(5)},
		{"Float", rng("$lt", 99.9)},
		{"AtBound", rng("$lte", int32(100))},
		{"Looser", rng("$lt", int32(1000))},
		{"String", "50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Index_PartialRange_" + tc.name,
				Support: harness.DumboDBFull,
				Setup:   partialRangeSetup,
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					filter := bson.D{{Key: "k", Value: int32(5)}, {Key: "grp", Value: tc.grp}}
					opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetProjection(bson.D{{Key: "_id", Value: 1}})
					cursor, err := col.Find(ctx, filter, opts)
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
