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
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// An $or whose every branch is served by an index is answered by a union of
// index scans; the results must match a collection scan's.

const orUnionDocs = 200

func orUnionSetup(ctx context.Context, col *mongo.Collection) error {
	docs := make([]interface{}, 0, orUnionDocs)
	for i := 0; i < orUnionDocs; i++ {
		d := bson.D{
			{Key: "_id", Value: int32(i)},
			{Key: "k", Value: int32((i * 7) % orUnionDocs)},
			{Key: "tags", Value: bson.A{fmt.Sprintf("t%d", i%10), fmt.Sprintf("t%d", i%7)}},
			{Key: "s", Value: fmt.Sprintf("s%03d", i)},
		}
		if i%50 == 0 {
			d = append(d, bson.E{Key: "n", Value: nil})
		}
		docs = append(docs, d)
	}
	if _, err := col.InsertMany(ctx, docs); err != nil {
		return err
	}
	for _, keys := range []bson.D{{{Key: "k", Value: 1}}, {{Key: "tags", Value: 1}}, {{Key: "s", Value: 1}}} {
		if err := createIndex(ctx, col, keys); err != nil {
			return err
		}
	}
	return nil
}

func TestIndex_OrUnion(t *testing.T) {
	or := func(branches ...bson.D) bson.D {
		arr := bson.A{}
		for _, b := range branches {
			arr = append(arr, b)
		}
		return bson.D{{Key: "$or", Value: arr}}
	}
	byID := bson.D{{Key: "_id", Value: 1}}
	for _, tc := range []struct {
		name   string
		filter bson.D
		sort   bson.D
		limit  int64
	}{
		{"EqEq", or(bson.D{{Key: "k", Value: int32(5)}}, bson.D{{Key: "s", Value: "s010"}}), byID, 0},
		{"Overlapping", or(bson.D{{Key: "k", Value: int32(7)}}, bson.D{{Key: "s", Value: "s001"}}), byID, 0},
		{"RangeAndIn", or(bson.D{{Key: "k", Value: bson.D{{Key: "$lt", Value: int32(5)}}}}, bson.D{{Key: "s", Value: bson.D{{Key: "$in", Value: bson.A{"s100", "s150"}}}}}), byID, 0},
		{"Multikey", or(bson.D{{Key: "tags", Value: "t3"}}, bson.D{{Key: "k", Value: int32(1)}}), byID, 0},
		{"UnindexedBranch", or(bson.D{{Key: "k", Value: int32(5)}}, bson.D{{Key: "n", Value: nil}}), byID, 0},
		{"ExtraPredicate", or(bson.D{{Key: "k", Value: int32(5)}, {Key: "s", Value: "nope"}}, bson.D{{Key: "s", Value: "s020"}}), byID, 0},
		{"SortLimit", or(bson.D{{Key: "tags", Value: "t3"}}, bson.D{{Key: "k", Value: bson.D{{Key: "$gte", Value: int32(190)}}}}), bson.D{{Key: "k", Value: -1}}, 5},
		{"NoMatch", or(bson.D{{Key: "k", Value: int32(-1)}}, bson.D{{Key: "s", Value: "zzz"}}), byID, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Index_OrUnion_" + tc.name,
				Support: harness.DumboDBFull,
				Setup:   orUnionSetup,
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					opts := options.Find().SetSort(tc.sort).SetProjection(bson.D{{Key: "_id", Value: 1}})
					if tc.limit > 0 {
						opts.SetLimit(tc.limit)
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
