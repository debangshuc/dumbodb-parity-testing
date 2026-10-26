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
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dolthub/dumbodb-parity-testing/shopdata"
)

// The compression benchmarks use the realistic orders cmd/gendata writes,
// because random payloads compress far worse than real documents; each seeds
// its own collection. The compressor under test is chosen by the target URI,
// e.g. ?compressors=snappy, so one benchmark covers every arm.
var targetPID = flag.Int("bench.target-pid", envInt("BENCH_TARGET_PID"), "server process id; when set, server CPU per op is reported")

func envInt(key string) int {
	v, _ := strconv.Atoi(os.Getenv(key))
	return v
}

// withSeededOrders returns a fresh collection holding the first n orders of
// the default shopdata dataset, with cmd/gendata's indexes on orders.
func withSeededOrders(b *testing.B, label string, n int) (*mongo.Collection, context.Context) {
	b.Helper()
	ctx := context.Background()
	client, err := connect(ctx)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	orders, cleanup := freshCollection(ctx, client, label)
	b.Cleanup(func() {
		cleanup()
		_ = client.Disconnect(context.Background())
	})
	docs := shopdata.Orders(n)
	const batch = 1000
	for i := 0; i < len(docs); i += batch {
		chunk := make([]interface{}, 0, batch)
		for _, doc := range docs[i:min(i+batch, len(docs))] {
			chunk = append(chunk, doc)
		}
		if _, err := orders.InsertMany(ctx, chunk, options.InsertMany().SetOrdered(false)); err != nil {
			b.Fatalf("seed orders: %v", err)
		}
	}
	if _, err := orders.Indexes().CreateMany(ctx, shopdata.OrderIndexes()); err != nil {
		b.Fatalf("index orders: %v", err)
	}
	return orders, ctx
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

// offClock runs f with the benchmark timer stopped and leaves the server CPU
// it uses out of server-cpu-ms/op.
func (m *compressionMetrics) offClock(f func()) {
	m.b.StopTimer()
	before := serverCPU(m.b)
	f()
	m.cpuStart += serverCPU(m.b) - before
	m.b.StartTimer()
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
	orders, ctx := withSeededOrders(b, "compression_findall", shopdata.DefaultOrders)
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
	ctx := context.Background()
	client, err := connect(ctx)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	scratch, cleanup := freshCollection(ctx, client, "compression_insertmany")
	b.Cleanup(func() {
		cleanup()
		_ = client.Disconnect(context.Background())
	})
	var batch []interface{}
	var batchBytes int64
	for _, order := range shopdata.Orders(1000) {
		raw, err := bson.Marshal(order)
		if err != nil {
			b.Fatalf("marshal order: %v", err)
		}
		batchBytes += int64(len(raw))
		// Without _id, so every insert gets fresh ids.
		batch = append(batch, order[1:])
	}

	clearInterval := insertResetDocs / len(batch)
	m := startCompressionMetrics(b)
	for i := 0; i < b.N; i++ {
		if i > 0 && i%clearInterval == 0 {
			m.offClock(func() {
				if _, err := scratch.DeleteMany(ctx, bson.D{}); err != nil {
					b.Fatalf("clear: %v", err)
				}
			})
		}
		start := time.Now()
		if _, err := scratch.InsertMany(ctx, batch); err != nil {
			b.Fatalf("insertMany: %v", err)
		}
		m.latencies = append(m.latencies, time.Since(start))
		m.bytes += batchBytes
	}
	m.report()
}

// BenchmarkCompression_PointRead fetches one order by its orderNumber, which
// cmd/gendata does not index: small messages, where compression only adds
// overhead.
func BenchmarkCompression_PointRead(b *testing.B) {
	orders, ctx := withSeededOrders(b, "compression_pointread", shopdata.DefaultOrders)
	n := int64(shopdata.DefaultOrders)
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
