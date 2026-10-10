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

package benchmarks

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Index-use benchmarks. A query that silently falls back to a collection scan
// returns correct results, so only latency would show it. Before timing, each
// benchmark asserts through explain (executionStats) that the winning plan uses
// the expected index and, for find, that the documents examined stay a small
// fraction of the collection. The assertion runs against MongoDB too, which
// validates it.

const indexUseDocs = 10000

// indexUseDoc is the fixture: flat, compound, dotted, deep dotted, and
// through-array keys, each selective.
func indexUseDoc(i int) bson.D {
	return bson.D{
		{Key: "_id", Value: int32(i)},
		{Key: "k", Value: int32(i)},
		{Key: "grp", Value: int32(i % 1000)},
		{Key: "a", Value: bson.D{
			{Key: "b", Value: int32(i)},
			{Key: "c", Value: bson.D{{Key: "d", Value: int32(i % 1000)}}},
		}},
		{Key: "items", Value: bson.A{
			bson.D{{Key: "sku", Value: fmt.Sprintf("sku-%d-0", i)}},
			bson.D{{Key: "sku", Value: fmt.Sprintf("sku-%d-1", i)}},
		}},
		{Key: "pad", Value: fmt.Sprintf("%0200d", i)},
	}
}

func withIndexUseCollection(b *testing.B, label string, keys bson.D) (*mongo.Collection, context.Context, bool) {
	b.Helper()
	return withIndexUseCollectionOf(b, label, keys, indexUseDoc)
}

func withIndexUseCollectionOf(b *testing.B, label string, keys bson.D, docOf func(i int) bson.D) (*mongo.Collection, context.Context, bool) {
	b.Helper()
	ctx := context.Background()
	client, err := connect(ctx)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	col, cleanup := freshCollection(ctx, client, label)
	b.Cleanup(func() {
		cleanup()
		_ = client.Disconnect(context.Background())
	})
	const batch = 1000
	buf := make([]interface{}, 0, batch)
	for i := 0; i < indexUseDocs; i++ {
		buf = append(buf, docOf(i))
		if len(buf) == batch || i == indexUseDocs-1 {
			if _, err := col.InsertMany(ctx, buf, options.InsertMany().SetOrdered(false)); err != nil {
				b.Fatalf("seed: %v", err)
			}
			buf = buf[:0]
		}
	}
	if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys, Options: options.Index().SetName("idx")}); err != nil {
		b.Fatalf("create index %v: %v", keys, err)
	}
	return col, ctx, isDumboDB(ctx, client)
}

// isDumboDB reports whether the target is DumboDB, which names its storage
// engine "dolt" in buildInfo.
func isDumboDB(ctx context.Context, client *mongo.Client) bool {
	var info struct {
		StorageEngines []string `bson:"storageEngines"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info); err != nil {
		return false
	}
	return slices.Contains(info.StorageEngines, "dolt")
}

// indexUseExpectation is either required, or a known DumboDB gap named by its
// tracking issue: the gap is logged while it persists and fails the benchmark
// once DumboDB starts using the index, so the expectation gets promoted.
type indexUseExpectation struct {
	knownDumboGap string
}

var mustUseIndex = indexUseExpectation{}

type explainSummary struct {
	stages       []string
	indexNames   []string
	indexesUsed  []string
	nReturned    int64
	docsExamined int64
	hasExecStats bool
}

func collectPlan(v interface{}, out *explainSummary) {
	m, ok := v.(bson.M)
	if !ok {
		return
	}
	if s, ok := m["stage"].(string); ok {
		out.stages = append(out.stages, s)
	}
	if n, ok := m["indexName"].(string); ok {
		out.indexNames = append(out.indexNames, n)
	}
	collectPlan(m["queryPlan"], out)
	collectPlan(m["inputStage"], out)
	if arr, ok := m["inputStages"].(bson.A); ok {
		for _, c := range arr {
			collectPlan(c, out)
		}
	}
}

func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func explainExecution(ctx context.Context, col *mongo.Collection, inner bson.D) (explainSummary, error) {
	var res bson.M
	err := col.Database().RunCommand(ctx, bson.D{
		{Key: "explain", Value: inner},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&res)
	if err != nil {
		return explainSummary{}, err
	}
	var out explainSummary
	collectExplain(res, &out)
	if stages, ok := res["stages"].(bson.A); ok {
		for _, st := range stages {
			stage, _ := st.(bson.M)
			for _, body := range stage {
				if m, ok := body.(bson.M); ok {
					collectExplain(m, &out)
				}
			}
			collectIndexesUsed(stage, &out)
		}
	}
	return out, nil
}

// collectExplain reads the winning plan and execution counts from res, which
// is an explain result or the $cursor stage of a classic aggregate explain.
func collectExplain(res bson.M, out *explainSummary) {
	if qp, ok := res["queryPlanner"].(bson.M); ok {
		collectPlan(qp["winningPlan"], out)
	}
	if es, ok := res["executionStats"].(bson.M); ok {
		n, nOK := asInt64(es["nReturned"])
		d, dOK := asInt64(es["totalDocsExamined"])
		out.nReturned, out.docsExamined, out.hasExecStats = n, d, nOK && dOK
	}
}

// collectIndexesUsed records the indexes a classic $lookup stage reports for
// its foreign side.
func collectIndexesUsed(stage bson.M, out *explainSummary) {
	used, _ := stage["indexesUsed"].(bson.A)
	for _, u := range used {
		if name, ok := u.(string); ok {
			out.indexesUsed = append(out.indexesUsed, name)
		}
	}
}

// indexScanStages are the plan stages that read an index. MongoDB's EXPRESS
// stages serve _id equality and EQ_LOOKUP a $lookup's foreign side.
var indexScanStages = []string{
	"IXSCAN", "COUNT_SCAN", "DISTINCT_SCAN",
	"EXPRESS_IXSCAN", "EXPRESS_UPDATE", "EXPRESS_DELETE", "EQ_LOOKUP",
}

// usesIndex reports whether s shows an index scan on idx and, when the command
// reports document counts for a find or aggregate, examined no more documents than it
// returned or a tenth of the collDocs-document collection, whichever is more.
func (s explainSummary) usesIndex(idx string, collDocs int, checkExamined bool) (bool, string) {
	scan := slices.ContainsFunc(s.stages, func(st string) bool { return slices.Contains(indexScanStages, st) })
	if !(scan && slices.Contains(s.indexNames, idx)) && !slices.Contains(s.indexesUsed, idx) {
		return false, fmt.Sprintf("plan %v on %v, want an index scan on %q", s.stages, s.indexNames, idx)
	}
	allowed := max(s.nReturned, int64(collDocs/10))
	if checkExamined && s.hasExecStats && s.docsExamined > allowed {
		return false, fmt.Sprintf("examined %d of %d docs for %d results", s.docsExamined, collDocs, s.nReturned)
	}
	return true, ""
}

// requireIndexUse fails b unless inner's plan uses the index-use fixture's
// index, honoring exp on DumboDB.
func requireIndexUse(b *testing.B, ctx context.Context, col *mongo.Collection, dumbo bool, inner bson.D, exp indexUseExpectation) {
	b.Helper()
	requireIndexUseOf(b, ctx, col, dumbo, inner, "idx", indexUseDocs, exp)
}

// requireIndexUseOf fails b unless inner's plan, on a collection of collDocs
// documents, uses the index named idx, honoring exp on DumboDB. A zero collDocs
// checks the plan only.
func requireIndexUseOf(b *testing.B, ctx context.Context, col *mongo.Collection, dumbo bool, inner bson.D, idx string, collDocs int, exp indexUseExpectation) {
	b.Helper()
	s, err := explainExecution(ctx, col, inner)
	if err != nil {
		b.Fatalf("explain %v: %v", inner, err)
	}
	checkExamined := collDocs > 0 && (inner[0].Key == "find" || inner[0].Key == "aggregate")
	ok, why := s.usesIndex(idx, collDocs, checkExamined)
	if ok && collDocs > 0 && readCommands[inner[0].Key] {
		ok, why = indexedReads(b, ctx, col, inner, collDocs)
	}
	switch {
	case ok && dumbo && exp.knownDumboGap != "":
		b.Fatalf("DumboDB now uses the index (%s); promote this benchmark to mustUseIndex", exp.knownDumboGap)
	case !ok && dumbo && exp.knownDumboGap != "":
		b.Logf("known DumboDB gap (%s): %s", exp.knownDumboGap, why)
	case !ok:
		b.Fatalf("index not used for %v: %s", inner, why)
	}
}

var readCommands = map[string]bool{"find": true, "aggregate": true, "count": true, "distinct": true}

// indexedReads runs inner and reports whether the server read every document
// through an index key and, for a find or a pipeline that returns documents
// unchanged, read no more documents than it returned or a tenth of the
// collDocs-document collection, whichever is more.
// Unlike explain, which plans separately, this measures the command itself.
func indexedReads(b *testing.B, ctx context.Context, col *mongo.Collection, inner bson.D, collDocs int) (bool, string) {
	b.Helper()
	r, err := readsOf(ctx, col, inner)
	if err != nil {
		b.Fatalf("run %v: %v", inner, err)
	}
	switch {
	case !r.tracked:
		b.Logf("server reports no queryExecutor counters; reads unchecked")
	case r.docs > r.keys:
		return false, fmt.Sprintf("read %d docs but only %d index keys: some came from a collection scan", r.docs, r.keys)
	case returnsReadDocuments(inner) && r.docs > max(r.returned, int64(collDocs/10)):
		return false, fmt.Sprintf("read %d of %d docs for %d results", r.docs, collDocs, r.returned)
	}
	return true, ""
}

// returnsReadDocuments reports whether cmd returns the documents it reads
// unchanged: a find, or a pipeline of only $match, $sort, and $limit.
func returnsReadDocuments(cmd bson.D) bool {
	switch cmd[0].Key {
	case "find":
		return true
	case "aggregate":
		for _, e := range cmd {
			if e.Key != "pipeline" {
				continue
			}
			for _, st := range e.Value.(bson.A) {
				switch st.(bson.D)[0].Key {
				case "$match", "$sort", "$limit":
				default:
					return false
				}
			}
			return true
		}
	}
	return false
}

// reads is the work a command did by serverStatus metrics.queryExecutor: index
// keys and documents read, and the documents it returned.
type reads struct {
	keys, docs, returned int64
	tracked              bool
}

// readsOf runs the read command inner to completion twice, keeping the lesser
// counts so background server work does not count. tracked is false when the
// server has no such counters.
func readsOf(ctx context.Context, col *mongo.Collection, inner bson.D) (reads, error) {
	best := reads{keys: -1, docs: -1}
	for range 2 {
		k0, d0, tracked, err := scannedCounters(ctx, col)
		if err != nil || !tracked {
			return reads{}, err
		}
		returned, err := runToCompletion(ctx, col, inner)
		if err != nil {
			return reads{}, err
		}
		k1, d1, _, err := scannedCounters(ctx, col)
		if err != nil {
			return reads{}, err
		}
		if best.keys < 0 || k1-k0 < best.keys {
			best.keys = k1 - k0
		}
		if best.docs < 0 || d1-d0 < best.docs {
			best.docs = d1 - d0
		}
		best.returned = returned
	}
	best.tracked = true
	return best, nil
}

func scannedCounters(ctx context.Context, col *mongo.Collection) (keys, docs int64, tracked bool, err error) {
	var res bson.M
	if err := col.Database().Client().Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&res); err != nil {
		return 0, 0, false, err
	}
	metrics, _ := res["metrics"].(bson.M)
	qe, _ := metrics["queryExecutor"].(bson.M)
	keys, keysOK := asInt64(qe["scanned"])
	docs, docsOK := asInt64(qe["scannedObjects"])
	return keys, docs, keysOK && docsOK, nil
}

// runToCompletion runs cmd, draining any cursor, and returns the documents it
// returned.
func runToCompletion(ctx context.Context, col *mongo.Collection, cmd bson.D) (int64, error) {
	if cmd[0].Key != "find" && cmd[0].Key != "aggregate" {
		return 1, col.Database().RunCommand(ctx, cmd).Err()
	}
	cur, err := col.Database().RunCommandCursor(ctx, cmd)
	if err != nil {
		return 0, err
	}
	var docs []bson.Raw
	err = cur.All(ctx, &docs)
	return int64(len(docs)), err
}

func findCmd(col *mongo.Collection, filter, sort bson.D, limit int64) bson.D {
	cmd := bson.D{{Key: "find", Value: col.Name()}, {Key: "filter", Value: filter}}
	if sort != nil {
		cmd = append(cmd, bson.E{Key: "sort", Value: sort})
	}
	if limit > 0 {
		cmd = append(cmd, bson.E{Key: "limit", Value: limit})
	}
	return cmd
}

func runFind(b *testing.B, ctx context.Context, col *mongo.Collection, filter, sort bson.D, limit int64) {
	opts := options.Find()
	if sort != nil {
		opts.SetSort(sort)
	}
	if limit > 0 {
		opts.SetLimit(limit)
	}
	cur, err := col.Find(ctx, filter, opts)
	if err != nil {
		b.Fatalf("Find: %v", err)
	}
	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		b.Fatalf("cursor.All: %v", err)
	}
}

// benchmarkIndexedFind times filter(i) after asserting filter(probe) uses the index.
func benchmarkIndexedFind(b *testing.B, label string, keys bson.D, filter func(i int) bson.D, sort bson.D, limit int64, exp indexUseExpectation) {
	benchmarkIndexedFindOf(b, label, keys, indexUseDoc, filter, sort, limit, exp)
}

func benchmarkIndexedFindOf(b *testing.B, label string, keys bson.D, docOf func(i int) bson.D, filter func(i int) bson.D, sort bson.D, limit int64, exp indexUseExpectation) {
	col, ctx, dumbo := withIndexUseCollectionOf(b, label, keys, docOf)
	requireIndexUse(b, ctx, col, dumbo, findCmd(col, filter(indexUseDocs/2), sort, limit), exp)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFind(b, ctx, col, filter(i%indexUseDocs), sort, limit)
	}
}

func eqOn(field string) func(int) bson.D {
	return func(i int) bson.D { return bson.D{{Key: field, Value: int32(i)}} }
}

func rangeOn(field string, width int) func(int) bson.D {
	return func(i int) bson.D {
		return bson.D{{Key: field, Value: bson.D{{Key: "$gte", Value: int32(i)}, {Key: "$lt", Value: int32(i + width)}}}}
	}
}

func inOn(field string) func(int) bson.D {
	return func(i int) bson.D {
		return bson.D{{Key: field, Value: bson.D{{Key: "$in", Value: bson.A{int32(i), int32(i + 7), int32(i + 13)}}}}}
	}
}

var (
	flatKey     = bson.D{{Key: "k", Value: 1}}
	compoundKey = bson.D{{Key: "grp", Value: 1}, {Key: "k", Value: 1}}
	dottedKey   = bson.D{{Key: "a.b", Value: 1}}
	deepKey     = bson.D{{Key: "a.c.d", Value: 1}}
	skuKey      = bson.D{{Key: "items.sku", Value: 1}}
)

func BenchmarkIndexUse_FlatEq_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_flat_eq", flatKey, eqOn("k"), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_FlatRange_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_flat_range", flatKey, rangeOn("k", 50), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_FlatIn_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_flat_in", flatKey, inOn("k"), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_CompoundPrefix_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_cmp_prefix", compoundKey, func(i int) bson.D {
		return bson.D{{Key: "grp", Value: int32(i % 1000)}}
	}, nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_CompoundFull_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_cmp_full", compoundKey, func(i int) bson.D {
		return bson.D{{Key: "grp", Value: int32(i % 1000)}, {Key: "k", Value: int32(i)}}
	}, nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_DottedEq_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_dot_eq", dottedKey, eqOn("a.b"), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_DottedRange_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_dot_range", dottedKey, rangeOn("a.b", 50), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_DottedIn_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_dot_in", dottedKey, inOn("a.b"), nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_DeepDottedEq_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_deep_eq", deepKey, func(i int) bson.D {
		return bson.D{{Key: "a.c.d", Value: int32(i % 1000)}}
	}, nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_ThroughArrayEq_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_arr_eq", skuKey, func(i int) bson.D {
		return bson.D{{Key: "items.sku", Value: fmt.Sprintf("sku-%d-1", i)}}
	}, nil, 0, mustUseIndex)
}

func BenchmarkIndexUse_FlatSortLimit_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_flat_sort", flatKey, func(int) bson.D { return bson.D{} },
		bson.D{{Key: "k", Value: 1}}, 10, mustUseIndex)
}

func BenchmarkIndexUse_FlatSortDescLimit_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_flat_sort_desc", flatKey, func(int) bson.D { return bson.D{} },
		bson.D{{Key: "k", Value: -1}}, 10, mustUseIndex)
}

func BenchmarkIndexUse_DottedSortLimit_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_dot_sort", dottedKey, func(int) bson.D { return bson.D{} },
		bson.D{{Key: "a.b", Value: 1}}, 10, mustUseIndex)
}

// A multikey index whose keys include empty, nested, and multi-element arrays:
// the sort reads the array-holding documents beside the walk.
func BenchmarkIndexUse_MultikeySortLimit_10K(b *testing.B) {
	docOf := func(i int) bson.D {
		var m interface{} = int32(i)
		switch i % 100 {
		case 0:
			m = bson.A{int32(i), int32(i + 5)}
		case 1:
			m = bson.A{}
		case 2:
			m = bson.A{bson.A{int32(i)}}
		}
		return append(indexUseDoc(i), bson.E{Key: "m", Value: m})
	}
	benchmarkIndexedFindOf(b, "iu_multikey_sort", bson.D{{Key: "m", Value: 1}}, docOf, func(int) bson.D { return bson.D{} },
		bson.D{{Key: "m", Value: 1}}, 10, mustUseIndex)
}

// Half the documents pass the unindexed filter, so walking the sort index
// reads about twice the limit.
func BenchmarkIndexUse_SortLimitUnindexedFilter_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_sort_unidx_filter", flatKey, func(int) bson.D {
		return bson.D{{Key: "grp", Value: bson.D{{Key: "$lt", Value: int32(500)}}}}
	}, bson.D{{Key: "k", Value: 1}}, 10, mustUseIndex)
}

// Every document in index order, across getMore batches.
func BenchmarkIndexUse_SortNoLimit_10K(b *testing.B) {
	benchmarkIndexedFind(b, "iu_sort_nolimit", flatKey, func(int) bson.D { return bson.D{} },
		bson.D{{Key: "k", Value: 1}}, 0, mustUseIndex)
}

// Plan choice between a filter's index and the sort's: with a limit, walking
// the sort index wins when many documents pass the filter, and the filter's
// index wins when few do. idx is the sort index on k; grpidx serves the filter.
func benchmarkSortOrFilterIndex(b *testing.B, label string, filter bson.D, want string) {
	col, ctx, dumbo := withIndexUseCollection(b, label, flatKey)
	if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "grp", Value: 1}}, Options: options.Index().SetName("grpidx")}); err != nil {
		b.Fatalf("create grp index: %v", err)
	}
	sort := bson.D{{Key: "k", Value: 1}}
	requireIndexUseOf(b, ctx, col, dumbo, findCmd(col, filter, sort, 10), want, indexUseDocs, mustUseIndex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFind(b, ctx, col, filter, sort, 10)
	}
}

func BenchmarkIndexUse_SortIndexBeatsUnselectiveFilter_10K(b *testing.B) {
	benchmarkSortOrFilterIndex(b, "iu_sort_vs_filter_wide", bson.D{{Key: "grp", Value: bson.D{{Key: "$lt", Value: int32(400)}}}}, "idx")
}

func BenchmarkIndexUse_FilterIndexBeatsSort_10K(b *testing.B) {
	benchmarkSortOrFilterIndex(b, "iu_sort_vs_filter_narrow", bson.D{{Key: "grp", Value: int32(7)}}, "grpidx")
}

// Hints: the filter binds both k (unique, idx) and grp (grpidx), so the
// planner alone would pick idx; the hint must force grpidx.
func benchmarkHintedFind(b *testing.B, label string, hint interface{}, check func(*testing.B, context.Context, *mongo.Collection, bool, bson.D)) {
	col, ctx, dumbo := withIndexUseCollection(b, label, flatKey)
	if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "grp", Value: 1}}, Options: options.Index().SetName("grpidx")}); err != nil {
		b.Fatalf("create grp index: %v", err)
	}
	filter := func(i int) bson.D { return bson.D{{Key: "k", Value: int32(i)}, {Key: "grp", Value: int32(i % 1000)}} }
	check(b, ctx, col, dumbo, append(findCmd(col, filter(indexUseDocs/2), nil, 0), bson.E{Key: "hint", Value: hint}))
	opts := options.Find().SetHint(hint)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Find(ctx, filter(i%indexUseDocs), opts)
		if err != nil {
			b.Fatalf("Find: %v", err)
		}
		var docs []bson.M
		if err := cur.All(ctx, &docs); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

func requireGrpIndex(b *testing.B, ctx context.Context, col *mongo.Collection, dumbo bool, inner bson.D) {
	b.Helper()
	requireIndexUseOf(b, ctx, col, dumbo, inner, "grpidx", indexUseDocs, mustUseIndex)
}

// requireCollectionScan fails b unless inner's plan is a collection scan that
// reads no index keys.
func requireCollectionScan(b *testing.B, ctx context.Context, col *mongo.Collection, _ bool, inner bson.D) {
	b.Helper()
	s, err := explainExecution(ctx, col, inner)
	if err != nil {
		b.Fatalf("explain %v: %v", inner, err)
	}
	if !slices.Contains(s.stages, "COLLSCAN") || len(s.indexNames) > 0 {
		b.Fatalf("plan %v on %v, want a collection scan", s.stages, s.indexNames)
	}
	r, err := readsOf(ctx, col, inner)
	if err != nil {
		b.Fatalf("run %v: %v", inner, err)
	}
	if r.tracked && r.keys > 0 {
		b.Fatalf("%v read %d index keys, want none", inner, r.keys)
	}
}

func BenchmarkIndexUse_HintByName_10K(b *testing.B) {
	benchmarkHintedFind(b, "iu_hint_name", "grpidx", requireGrpIndex)
}

func BenchmarkIndexUse_HintByKeyPattern_10K(b *testing.B) {
	benchmarkHintedFind(b, "iu_hint_keys", bson.D{{Key: "grp", Value: 1}}, requireGrpIndex)
}

func BenchmarkIndexUse_HintNatural_10K(b *testing.B) {
	benchmarkHintedFind(b, "iu_hint_natural", bson.D{{Key: "$natural", Value: 1}}, requireCollectionScan)
}

// An equality on a compound index's leading field leaves the scan in order of
// the next field, so a sort on it with a limit reads only the limit's
// documents. Each bucket holds a fifth of the collection, so fetching every
// match and sorting fails the examined-documents check.
func BenchmarkIndexUse_CompoundPrefixSortLimit_10K(b *testing.B) {
	keys := bson.D{{Key: "bucket", Value: 1}, {Key: "k", Value: 1}}
	col, ctx, dumbo := withIndexUseCollectionOf(b, "iu_cmp_prefix_sort", keys, func(i int) bson.D {
		return append(indexUseDoc(i), bson.E{Key: "bucket", Value: int32(i % 5)})
	})
	filter := func(i int) bson.D { return bson.D{{Key: "bucket", Value: int32(i % 5)}} }
	sort := bson.D{{Key: "k", Value: -1}}
	requireIndexUse(b, ctx, col, dumbo, findCmd(col, filter(0), sort, 10), mustUseIndex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runFind(b, ctx, col, filter(i), sort, 10)
	}
}

// benchmarkIndexedAggregate times pipeline(i) after asserting pipeline(probe)
// uses the index.
func benchmarkIndexedAggregate(b *testing.B, label string, keys bson.D, pipeline func(i int) bson.A) {
	col, ctx, dumbo := withIndexUseCollection(b, label, keys)
	requireIndexUse(b, ctx, col, dumbo, aggregateCmd(col, pipeline(indexUseDocs/2)), mustUseIndex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, err := col.Aggregate(ctx, pipeline(i%indexUseDocs))
		if err != nil {
			b.Fatalf("Aggregate: %v", err)
		}
		var out []bson.M
		if err := cur.All(ctx, &out); err != nil {
			b.Fatalf("cursor.All: %v", err)
		}
	}
}

func matchStage(filter bson.D) bson.D { return bson.D{{Key: "$match", Value: filter}} }

var (
	sortKStage   = bson.D{{Key: "$sort", Value: bson.D{{Key: "k", Value: -1}}}}
	limit10Stage = bson.D{{Key: "$limit", Value: 10}}
)

func BenchmarkIndexUse_AggMatch_10K(b *testing.B) {
	benchmarkIndexedAggregate(b, "iu_agg_match", flatKey, func(i int) bson.A {
		return bson.A{matchStage(eqOn("k")(i))}
	})
}

func BenchmarkIndexUse_AggSortLimit_10K(b *testing.B) {
	benchmarkIndexedAggregate(b, "iu_agg_sort", flatKey, func(int) bson.A {
		return bson.A{sortKStage, limit10Stage}
	})
}

func BenchmarkIndexUse_AggMatchSortLimit_10K(b *testing.B) {
	benchmarkIndexedAggregate(b, "iu_agg_match_sort", flatKey, func(i int) bson.A {
		return bson.A{matchStage(rangeOn("k", 1000)(i)), sortKStage, limit10Stage}
	})
}

func BenchmarkIndexUse_AggMatchGroup_10K(b *testing.B) {
	benchmarkIndexedAggregate(b, "iu_agg_match_group", flatKey, func(i int) bson.A {
		return bson.A{matchStage(rangeOn("k", 50)(i)), bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$grp"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}}}
	})
}

func benchmarkIndexedCount(b *testing.B, label string, keys bson.D, filter func(i int) bson.D) {
	col, ctx, dumbo := withIndexUseCollection(b, label, keys)
	cmd := func(i int) bson.D {
		return bson.D{{Key: "count", Value: col.Name()}, {Key: "query", Value: filter(i)}}
	}
	requireIndexUse(b, ctx, col, dumbo, cmd(indexUseDocs/2), mustUseIndex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.Database().RunCommand(ctx, cmd(i%indexUseDocs)).Err(); err != nil {
			b.Fatalf("count: %v", err)
		}
	}
}

func BenchmarkIndexUse_FlatCount_10K(b *testing.B) {
	benchmarkIndexedCount(b, "iu_flat_count", flatKey, eqOn("k"))
}

func BenchmarkIndexUse_DottedCount_10K(b *testing.B) {
	benchmarkIndexedCount(b, "iu_dot_count", dottedKey, eqOn("a.b"))
}

func benchmarkIndexedDistinct(b *testing.B, label string, keys bson.D, field string) {
	col, ctx, dumbo := withIndexUseCollection(b, label, keys)
	cmd := bson.D{{Key: "distinct", Value: col.Name()}, {Key: "key", Value: field}, {Key: "query", Value: bson.D{}}}
	requireIndexUse(b, ctx, col, dumbo, cmd, mustUseIndex)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Distinct(ctx, field, bson.D{}); err != nil {
			b.Fatalf("Distinct: %v", err)
		}
	}
}

func BenchmarkIndexUse_FlatDistinct_10K(b *testing.B) {
	benchmarkIndexedDistinct(b, "iu_flat_distinct", bson.D{{Key: "grp", Value: 1}}, "grp")
}

func BenchmarkIndexUse_DottedDistinct_10K(b *testing.B) {
	benchmarkIndexedDistinct(b, "iu_dot_distinct", deepKey, "a.c.d")
}

// Write paths: inserts maintain each index kind; dotted and through-array keys
// resolve their paths through every document.
func benchmarkIndexedInsert(b *testing.B, label string, keys bson.D, unique bool) {
	ctx := context.Background()
	client, err := connect(ctx)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	col, cleanup := freshCollection(ctx, client, label)
	b.Cleanup(func() {
		cleanup()
		_ = client.Disconnect(context.Background())
	})
	if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: keys, Options: options.Index().SetUnique(unique)}); err != nil {
		b.Fatalf("create index: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.InsertOne(ctx, indexUseDoc(i)); err != nil {
			b.Fatalf("InsertOne: %v", err)
		}
	}
}

func BenchmarkIndexUse_InsertUniqueFlat(b *testing.B) {
	benchmarkIndexedInsert(b, "iu_ins_flat", flatKey, true)
}

func BenchmarkIndexUse_InsertUniqueDotted(b *testing.B) {
	benchmarkIndexedInsert(b, "iu_ins_dot", dottedKey, true)
}

func BenchmarkIndexUse_InsertUniqueThroughArray(b *testing.B) {
	benchmarkIndexedInsert(b, "iu_ins_arr", skuKey, true)
}
