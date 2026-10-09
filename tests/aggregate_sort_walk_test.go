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

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// A pipeline led by $sort (optionally after $match) on an indexed field is
// answered by walking the index, so a following $limit stops the scan early.

func TestAggregate_SortWalk(t *testing.T) {
	asc := bson.D{{Key: "k", Value: 1}}
	desc := bson.D{{Key: "k", Value: -1}}
	sortStage := func(s bson.D) bson.D { return bson.D{{Key: "$sort", Value: s}} }
	limit := bson.D{{Key: "$limit", Value: int32(5)}}
	match := bson.D{{Key: "$match", Value: bson.D{{Key: "p", Value: "p1"}}}}
	for _, tc := range []struct {
		name     string
		keys     bson.D
		pipeline bson.A
		support  harness.DumboDBSupport
	}{
		{"AscLimit", asc, bson.A{sortStage(asc), limit}, harness.DumboDBXFail},
		{"DescLimit", asc, bson.A{sortStage(desc), limit}, harness.DumboDBXFail},
		{"UnindexedMatch", asc, bson.A{match, sortStage(asc), limit}, harness.DumboDBXFail},
		{"SortThenMatch", asc, bson.A{sortStage(asc), match, limit}, harness.DumboDBXFail},
		{"NoIndexControl", nil, bson.A{sortStage(asc), limit}, harness.DumboDBXFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Aggregate_SortWalk_" + tc.name,
				Support: tc.support,
				Setup:   sortWalkSetup(tc.keys),
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					cur, err := col.Aggregate(ctx, tc.pipeline)
					if err != nil {
						return nil, err
					}
					var docs []bson.D
					if err := cur.All(ctx, &docs); err != nil {
						return nil, err
					}
					ids := bson.A{}
					for _, d := range docs {
						ids = append(ids, d[0].Value)
					}
					explain, err := explainRunExplain(ctx, col, bson.D{
						{Key: "aggregate", Value: col.Name()},
						{Key: "pipeline", Value: tc.pipeline},
						{Key: "cursor", Value: bson.D{}},
					}, "executionStats")
					if err != nil {
						return nil, err
					}
					return bson.D{{Key: "ids", Value: ids}, {Key: "explain", Value: explainExtractCritical(explain)}}, nil
				},
			})
		})
	}
}
