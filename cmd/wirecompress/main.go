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

// Command wirecompress measures how compressible a server's real wire traffic
// is. It relays a fixed workload through a recording proxy to one target, then
// compresses every captured message with snappy, zlib and zstd and reports wire
// bytes per workload, direction and command, plus codec throughput.
//
// Run it once per target (mongod, dumbodb) and compare the CSVs. The data comes
// from cmd/gendata, run through the proxy, so both targets hold identical data
// and its inserts are measured as the bulk-insert workload.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	target := flag.String("target", "127.0.0.1:27017", "host:port of the server to measure")
	name := flag.String("name", "mongod", "label for the target in the output")
	gendata := flag.String("gendata", "", "path to a built cmd/gendata binary (required)")
	orders := flag.Int("orders", 10000, "orders gendata generates")
	dolt := flag.Bool("dolt", false, "also run the DumboDB-only doltLog and doltDiff workloads")
	rounds := flag.Int("rounds", 5, "timing passes per codec; the fastest is reported")
	out := flag.String("csv", "", "write per-command results to this CSV file")
	flag.Parse()
	if *gendata == "" {
		log.Fatal("-gendata is required")
	}

	rec := newRecorder(*target)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go rec.serve(ln)
	proxyURI := "mongodb://" + ln.Addr().String() + "/?directConnection=true"

	ctx := context.Background()
	rec.setWorkload("seed_gendata")
	seed := exec.Command(*gendata, "-uri", proxyURI, "-db", "shop", "-orders", fmt.Sprint(*orders))
	seed.Stdout, seed.Stderr = os.Stderr, os.Stderr
	if err := seed.Run(); err != nil {
		log.Fatalf("gendata: %v", err)
	}

	rec.setWorkload("setup")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(proxyURI))
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database("shop")
	if err := runWorkloads(ctx, rec, db, *orders, *dolt); err != nil {
		log.Fatal(err)
	}
	_ = client.Disconnect(ctx)
	_ = ln.Close()

	rec.mu.Lock()
	messages := rec.messages
	rec.mu.Unlock()
	groups, cpu, err := analyze(messages, *rounds)
	if err != nil {
		log.Fatal(err)
	}
	printSummary(*name, groups, cpu)
	if *out != "" {
		if err := writeCSV(*out, *name, groups, cpu); err != nil {
			log.Fatal(err)
		}
	}
}

func orderNumber(i int) string { return fmt.Sprintf("ORD-%07d", i) }

func runWorkloads(ctx context.Context, rec *recorder, db *mongo.Database, orders int, dolt bool) error {
	r := rand.New(rand.NewSource(7))
	coll := db.Collection("orders")

	rec.setWorkload("point_reads")
	for i := 0; i < 2000; i++ {
		var doc bson.M
		if err := coll.FindOne(ctx, bson.D{{Key: "orderNumber", Value: orderNumber(r.Intn(orders))}}).Decode(&doc); err != nil {
			return fmt.Errorf("point read: %w", err)
		}
	}

	rec.setWorkload("point_updates")
	for i := 0; i < 1000; i++ {
		_, err := coll.UpdateOne(ctx,
			bson.D{{Key: "orderNumber", Value: orderNumber(r.Intn(orders))}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "shipped"}}}})
		if err != nil {
			return fmt.Errorf("point update: %w", err)
		}
	}

	rec.setWorkload("find_all")
	if err := drain(coll.Find(ctx, bson.D{})); err != nil {
		return fmt.Errorf("find all: %w", err)
	}

	rec.setWorkload("aggregate")
	pipelines := []bson.A{
		{bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "delivered"}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "orderDate", Value: -1}}}},
			bson.D{{Key: "$limit", Value: 500}}},
		{bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$status"},
			{Key: "orders", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "subtotal", Value: bson.D{{Key: "$sum", Value: "$subtotal"}}}}}}},
	}
	for _, p := range pipelines {
		if err := drain(coll.Aggregate(ctx, p)); err != nil {
			return fmt.Errorf("aggregate: %w", err)
		}
	}

	if dolt {
		return runDoltWorkloads(ctx, rec, db, r, orders)
	}
	return nil
}

func runDoltWorkloads(ctx context.Context, rec *recorder, db *mongo.Database, r *rand.Rand, orders int) error {
	coll := db.Collection("orders")
	commit := func(message string) (string, error) {
		var reply bson.M
		if err := db.RunCommand(ctx, bson.D{{Key: "doltCommit", Value: 1}, {Key: "message", Value: message}}).Decode(&reply); err != nil {
			return "", fmt.Errorf("doltCommit: %w", err)
		}
		id, _ := reply["commitId"].(string)
		return id, nil
	}

	rec.setWorkload("dolt_setup")
	first, err := commit("seed")
	if err != nil {
		return err
	}
	for round := 0; round < 5; round++ {
		numbers := bson.A{}
		for i := 0; i < 200; i++ {
			numbers = append(numbers, orderNumber(r.Intn(orders)))
		}
		_, err := coll.UpdateMany(ctx,
			bson.D{{Key: "orderNumber", Value: bson.D{{Key: "$in", Value: numbers}}}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "refunded"}, {Key: "auditRound", Value: round}}}})
		if err != nil {
			return fmt.Errorf("dolt round update: %w", err)
		}
		if _, err := commit(fmt.Sprintf("round %d", round)); err != nil {
			return err
		}
	}

	commands := []struct {
		workload string
		command  bson.D
	}{
		{"dolt_log", bson.D{{Key: "doltLog", Value: 1}, {Key: "limit", Value: 10}}},
		{"dolt_log_patch", bson.D{{Key: "doltLog", Value: 1}, {Key: "limit", Value: 5}, {Key: "patch", Value: true}}},
		{"dolt_diff", bson.D{{Key: "doltDiff", Value: 1}, {Key: "from", Value: first}, {Key: "to", Value: "HEAD"}}},
	}
	for _, c := range commands {
		rec.setWorkload(c.workload)
		if err := db.RunCommand(ctx, c.command).Err(); err != nil {
			return fmt.Errorf("%s: %w", c.workload, err)
		}
	}
	return nil
}

func drain(cursor *mongo.Cursor, err error) error {
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer cursor.Close(ctx)
	deadline, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for cursor.Next(deadline) {
	}
	return cursor.Err()
}
