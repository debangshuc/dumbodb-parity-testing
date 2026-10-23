package benchmarks

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Projection benchmarks return a few small fields of each document. With 10KB
// documents almost all of each document is the payload, which the projection
// discards.

func benchmarkFindProjection(b *testing.B, label string, size docSize, n int, filter func(i int) bson.D, index string, sort bson.D) {
	col, ctx := withSeededCollection(b, label, n, size)
	if index != "" {
		if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: index, Value: 1}}}); err != nil {
			b.Fatalf("CreateOne: %v", err)
		}
	}
	opts := options.Find().SetProjection(bson.D{{Key: "i", Value: 1}, {Key: "tag", Value: 1}})
	if sort != nil {
		opts.SetSort(sort)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Find(ctx, filter(i), opts)
		if err != nil {
			b.Fatalf("Find: %v", err)
		}
		var docs []bson.M
		if err := cur.All(ctx, &docs); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

func noFilter(int) bson.D { return bson.D{} }

// grpFilter matches one of ten grp values, ~10% of the collection.
func grpFilter(i int) bson.D { return bson.D{{Key: "grp", Value: i % 10}} }

func BenchmarkFind_Projection(b *testing.B) {
	benchmarkFindProjection(b, "findproj", benchDocSize, benchDatasetSize, noFilter, "", nil)
}

func BenchmarkFind_Projection_10KB(b *testing.B) {
	benchmarkFindProjection(b, "findproj_10kb", sizeLarge, datasetSizeFor(sizeLarge), noFilter, "", nil)
}

func BenchmarkFind_Projection_FilterEq_10KB(b *testing.B) {
	benchmarkFindProjection(b, "findproj_eq_10kb", sizeLarge, datasetSizeFor(sizeLarge), grpFilter, "", nil)
}

func BenchmarkFind_Projection_FilterEq_10KB_Indexed(b *testing.B) {
	benchmarkFindProjection(b, "findproj_eq_10kb_idx", sizeLarge, datasetSizeFor(sizeLarge), grpFilter, "grp", nil)
}

func BenchmarkFind_Projection_FilterEq_Sort_10KB_Indexed(b *testing.B) {
	benchmarkFindProjection(b, "findproj_eq_sort_10kb_idx", sizeLarge, datasetSizeFor(sizeLarge), grpFilter, "grp", bson.D{{Key: "grp", Value: 1}})
}

func BenchmarkFind_Projection_Sort_10KB_Indexed(b *testing.B) {
	benchmarkFindProjection(b, "findproj_sort_10kb_idx", sizeLarge, datasetSizeFor(sizeLarge), grpFilter, "i", bson.D{{Key: "i", Value: 1}})
}

func BenchmarkAgg_Project_10KB(b *testing.B) {
	col, ctx := withSeededCollection(b, "aggproj_10kb", datasetSizeFor(sizeLarge), sizeLarge)
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
		var docs []bson.M
		if err := cur.All(ctx, &docs); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}
