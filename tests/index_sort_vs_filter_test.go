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

// When the filter can use one index and the sort another, MongoDB picks
// between an index scan on the filter followed by a blocking SORT and a walk
// of the sort field's index that filters as it goes, by which plan produces
// results faster: an unselective filter with a small limit favors the walk, a
// selective filter favors its own index.

const sortVsFilterDocs = 3000

func sortVsFilterSetup(ctx context.Context, col *mongo.Collection) error {
	docs := make([]interface{}, 0, sortVsFilterDocs)
	for i := 0; i < sortVsFilterDocs; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: int32(i)},
			{Key: "k", Value: int32((i * 7) % sortVsFilterDocs)},
			{Key: "p", Value: fmt.Sprintf("p%d", i%3)},
			{Key: "q", Value: int32(i % 1000)},
		})
	}
	if _, err := col.InsertMany(ctx, docs); err != nil {
		return err
	}
	for _, f := range []string{"k", "p", "q"} {
		if err := createIndex(ctx, col, bson.D{{Key: f, Value: 1}}); err != nil {
			return err
		}
	}
	return nil
}

func TestIndex_SortVsFilter(t *testing.T) {
	unselective := bson.D{{Key: "p", Value: "p1"}}
	selective := bson.D{{Key: "q", Value: int32(7)}}
	qBelow := func(n int32) bson.D { return bson.D{{Key: "q", Value: bson.D{{Key: "$lt", Value: n}}}} }
	for _, tc := range []struct {
		name    string
		filter  bson.D
		sortDir int32
		limit   int64
		support harness.DumboDBSupport
	}{
		{"UnselectiveLimit", unselective, 1, 5, harness.DumboDBFull},
		{"UnselectiveDescLimit", unselective, -1, 5, harness.DumboDBFull},
		{"UnselectiveNoLimit", unselective, 1, 0, harness.DumboDBFull},
		{"SelectiveLimit", selective, 1, 5, harness.DumboDBFull},
		{"SelectiveNoLimit", selective, 1, 0, harness.DumboDBFull},
		{"Moderate300Limit", qBelow(100), 1, 5, harness.DumboDBFull},
		{"Moderate300NoLimit", qBelow(100), 1, 0, harness.DumboDBFull},
		{"Narrow90Limit", qBelow(30), 1, 5, harness.DumboDBFull},
		{"Narrow150Limit", qBelow(50), 1, 5, harness.DumboDBFull},
		{"Wide1500NoLimit", qBelow(500), 1, 0, harness.DumboDBFull},
		{"EmptyLimit", bson.D{{Key: "q", Value: int32(-1)}}, 1, 5, harness.DumboDBFull},
		{"TinyIn", bson.D{{Key: "q", Value: bson.D{{Key: "$in", Value: bson.A{int32(0), int32(1)}}}}}, 1, 5, harness.DumboDBFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    "Index_SortVsFilter_" + tc.name,
				Support: tc.support,
				Setup:   sortVsFilterSetup,
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					sort := bson.D{{Key: "k", Value: tc.sortDir}}
					opts := options.Find().SetSort(sort).SetProjection(bson.D{{Key: "_id", Value: 1}})
					cmd := bson.D{
						{Key: "find", Value: col.Name()},
						{Key: "filter", Value: tc.filter},
						{Key: "sort", Value: sort},
					}
					if tc.limit > 0 {
						opts.SetLimit(tc.limit)
						cmd = append(cmd, bson.E{Key: "limit", Value: int32(tc.limit)})
					}
					cur, err := col.Find(ctx, tc.filter, opts)
					if err != nil {
						return nil, err
					}
					var docs []bson.D
					if err := cur.All(ctx, &docs); err != nil {
						return nil, err
					}
					explain, err := explainRunExplain(ctx, col, cmd, "executionStats")
					if err != nil {
						return nil, err
					}
					critical := explainExtractCritical(explain)
					return bson.D{
						{Key: "n", Value: len(docs)},
						{Key: "first", Value: firstIDs(docs, 5)},
						{Key: "winningPlan", Value: critical.Map()["winningPlan"]},
						{Key: "nReturned", Value: lookupStat(critical, "nReturned")},
						{Key: "docsExamined", Value: lookupStat(critical, "totalDocsExamined")},
					}, nil
				},
			})
		})
	}
}

func firstIDs(docs []bson.D, n int) bson.A {
	ids := bson.A{}
	for i := 0; i < len(docs) && i < n; i++ {
		ids = append(ids, docs[i][0].Value)
	}
	return ids
}
