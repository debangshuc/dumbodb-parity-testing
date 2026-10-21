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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// The compression benchmarks read the realistic dataset cmd/gendata writes,
// because random payloads compress far worse than real documents. Seed it on
// the target first (gendata -db shop). The compressor under test is chosen by
// the target URI, e.g. ?compressors=snappy, so one benchmark covers every arm.
var (
	shopDB    = flag.String("bench.shop-db", envOr("BENCH_SHOP_DB", "shop"), "database seeded by cmd/gendata")
	targetPID = flag.Int("bench.target-pid", envInt("BENCH_TARGET_PID"), "server process id; when set, server CPU per op is reported")
)

func envInt(key string) int {
	v, _ := strconv.Atoi(os.Getenv(key))
	return v
}

func withShopOrders(b *testing.B) (*mongo.Client, *mongo.Collection, context.Context) {
	b.Helper()
	ctx := context.Background()
	client, err := connect(ctx)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	b.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	orders := client.Database(*shopDB).Collection("orders")
	n, err := orders.EstimatedDocumentCount(ctx)
	if err != nil || n == 0 {
		b.Fatalf("%s.orders is empty or unreadable (%v); seed it with cmd/gendata first", *shopDB, err)
	}
	return client, orders, ctx
}

// serverCPU returns the target process's user+system CPU time, or 0 when no
// pid is configured. /proc reports it in clock ticks, which are 10ms on Linux.
func serverCPU(b *testing.B) time.Duration {
	if *targetPID == 0 {
		return 0
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", *targetPID))
	if err != nil {
		b.Fatalf("read server cpu: %v", err)
	}
	// Fields after the parenthesised command name; utime and stime are 14 and 15.
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+2:]))
	utime, _ := strconv.ParseInt(fields[11], 10, 64)
	stime, _ := strconv.ParseInt(fields[12], 10, 64)
	return time.Duration(utime+stime) * 10 * time.Millisecond
}

type compressionMetrics struct {
	b         *testing.B
	cpuStart  time.Duration
	bytes     int64
	latencies []time.Duration
}

func startCompressionMetrics(b *testing.B) *compressionMetrics {
	m := &compressionMetrics{b: b, cpuStart: serverCPU(b)}
	b.ResetTimer()
	return m
}

func (m *compressionMetrics) report() {
	m.b.StopTimer()
	if m.bytes > 0 {
		m.b.ReportMetric(float64(m.bytes)/1e6/m.b.Elapsed().Seconds(), "doc-MB/s")
	}
	if len(m.latencies) > 0 {
		sort.Slice(m.latencies, func(i, j int) bool { return m.latencies[i] < m.latencies[j] })
		pct := func(p float64) float64 {
			return float64(m.latencies[int(p*float64(len(m.latencies)-1))].Microseconds()) / 1000
		}
		m.b.ReportMetric(pct(0.50), "p50-ms")
		m.b.ReportMetric(pct(0.99), "p99-ms")
	}
	if *targetPID != 0 {
		m.b.ReportMetric(float64((serverCPU(m.b)-m.cpuStart).Microseconds())/1000/float64(m.b.N), "server-cpu-ms/op")
	}
}

// BenchmarkCompression_FindAllOrders streams every order: large replies.
func BenchmarkCompression_FindAllOrders(b *testing.B) {
	_, orders, ctx := withShopOrders(b)
	m := startCompressionMetrics(b)
	for i := 0; i < b.N; i++ {
		start := time.Now()
		cursor, err := orders.Find(ctx, bson.D{})
		if err != nil {
			b.Fatalf("find: %v", err)
		}
		for cursor.Next(ctx) {
			m.bytes += int64(len(cursor.Current))
		}
		if err := cursor.Err(); err != nil {
			b.Fatalf("find: %v", err)
		}
		_ = cursor.Close(ctx)
		m.latencies = append(m.latencies, time.Since(start))
	}
	m.report()
}

// BenchmarkCompression_InsertManyOrders inserts 1,000 copies of real orders per
// op into a scratch collection: large requests.
func BenchmarkCompression_InsertManyOrders(b *testing.B) {
	client, orders, ctx := withShopOrders(b)
	cursor, err := orders.Find(ctx, bson.D{})
	if err != nil {
		b.Fatalf("load orders: %v", err)
	}
	var batch []interface{}
	var batchBytes int64
	for len(batch) < 1000 && cursor.Next(ctx) {
		var doc bson.D
		if err := bson.Unmarshal(cursor.Current, &doc); err != nil {
			b.Fatalf("decode order: %v", err)
		}
		withoutID := make(bson.D, 0, len(doc))
		for _, e := range doc {
			if e.Key != "_id" {
				withoutID = append(withoutID, e)
			}
		}
		batch = append(batch, withoutID)
		batchBytes += int64(len(cursor.Current))
	}
	_ = cursor.Close(ctx)
	if len(batch) == 0 {
		b.Fatal("no orders to insert")
	}
	scratch, cleanup := freshCollection(ctx, client, "compression_insertmany")
	b.Cleanup(cleanup)

	m := startCompressionMetrics(b)
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := scratch.InsertMany(ctx, batch); err != nil {
			b.Fatalf("insertMany: %v", err)
		}
		m.latencies = append(m.latencies, time.Since(start))
		m.bytes += batchBytes
	}
	m.report()
}

// BenchmarkCompression_PointRead fetches one order by its indexed orderNumber:
// small messages, where compression only adds overhead.
func BenchmarkCompression_PointRead(b *testing.B) {
	_, orders, ctx := withShopOrders(b)
	n, _ := orders.EstimatedDocumentCount(ctx)
	r := rand.New(rand.NewSource(*dataSeed))
	m := startCompressionMetrics(b)
	for i := 0; i < b.N; i++ {
		start := time.Now()
		var doc bson.Raw
		filter := bson.D{{Key: "orderNumber", Value: fmt.Sprintf("ORD-%07d", r.Int63n(n))}}
		if err := orders.FindOne(ctx, filter).Decode(&doc); err != nil {
			b.Fatalf("findOne: %v", err)
		}
		m.latencies = append(m.latencies, time.Since(start))
	}
	m.report()
}
