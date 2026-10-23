package benchmarks

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// Aggregation benchmarks use the same 1K small-doc dataset as the CRUD suite.
// Each iteration drains the cursor so we measure end-to-end pipeline cost, not
// just the first-batch dispatch.

func BenchmarkAgg_MatchGroup(b *testing.B) {
	col, ctx := withSeededCollection(b, "agg_matchgroup", benchDatasetSize, benchDocSize)
	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "grp", Value: bson.D{{Key: "$gte", Value: 0}}}}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$grp"},
			{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Aggregate(ctx, pipeline)
		if err != nil {
			b.Fatalf("Aggregate: %v", err)
		}
		var out []bson.M
		if err := cur.All(ctx, &out); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

var sortLimitPipeline = bson.A{
	bson.D{{Key: "$sort", Value: bson.D{{Key: "i", Value: -1}}}},
	bson.D{{Key: "$limit", Value: 100}},
}

func BenchmarkAgg_Sort(b *testing.B) {
	col, ctx := withSeededCollection(b, "agg_sort", benchDatasetSize, benchDocSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Aggregate(ctx, sortLimitPipeline)
		if err != nil {
			b.Fatalf("Aggregate: %v", err)
		}
		var out []bson.M
		if err := cur.All(ctx, &out); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

// The same $sort + $limit at 10K, where reading only the limit's documents
// from an index on the sort field clearly beats sorting the collection.
func benchmarkAggSortScaled(b *testing.B, n int, indexed bool) {
	label := fmt.Sprintf("agg_sort_%d", n)
	if indexed {
		label += "_idx"
	}
	col, ctx := withSeededCollection(b, label, n, sizeSmall)
	if indexed {
		idx := createIndex(b, ctx, col, bson.D{{Key: "i", Value: 1}})
		requireIndexed(b, ctx, col, aggregateCmd(col, sortLimitPipeline), idx, n, mustUseIndex)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Aggregate(ctx, sortLimitPipeline)
		if err != nil {
			b.Fatalf("Aggregate: %v", err)
		}
		var out []bson.M
		if err := cur.All(ctx, &out); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

func BenchmarkAgg_Sort_10K(b *testing.B)         { benchmarkAggSortScaled(b, 10000, false) }
func BenchmarkAgg_Sort_10K_Indexed(b *testing.B) { benchmarkAggSortScaled(b, 10000, true) }

func BenchmarkAgg_Project(b *testing.B) {
	col, ctx := withSeededCollection(b, "agg_project", benchDatasetSize, benchDocSize)
	pipeline := bson.A{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 1},
			{Key: "grp", Value: 1},
			{Key: "tag", Value: 1},
		}}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Aggregate(ctx, pipeline)
		if err != nil {
			b.Fatalf("Aggregate: %v", err)
		}
		var out []bson.M
		if err := cur.All(ctx, &out); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}
