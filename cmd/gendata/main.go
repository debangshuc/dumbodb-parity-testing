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

package main

import (
	"context"
	"flag"
	"log"
	"math/rand"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dolthub/dumbodb-parity-testing/shopdata"
)

func main() {
	uri := flag.String("uri", "mongodb://127.0.0.1:27017", "MongoDB connection URI of the server to populate")
	dbName := flag.String("db", "shop", "database to populate")
	nCustomers := flag.Int("customers", shopdata.DefaultCustomers, "number of customer documents")
	nProducts := flag.Int("products", shopdata.DefaultProducts, "number of product documents")
	nOrders := flag.Int("orders", shopdata.DefaultOrders, "number of order documents")
	batch := flag.Int("batch", 1000, "insert batch size")
	seed := flag.Int64("seed", shopdata.DefaultSeed, "PRNG seed for reproducible data")
	drop := flag.Bool("drop", true, "drop the target collections before generating")
	flag.Parse()

	r := rand.New(rand.NewSource(*seed))
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	if err != nil {
		log.Fatalf("connect %s: %v", *uri, err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("ping %s: %v", *uri, err)
	}

	db := client.Database(*dbName)
	customers := db.Collection("customers")
	products := db.Collection("products")
	orders := db.Collection("orders")

	if *drop {
		for _, c := range []*mongo.Collection{customers, products, orders} {
			if err := c.Drop(ctx); err != nil {
				log.Fatalf("drop %s: %v", c.Name(), err)
			}
		}
	}

	inserter := func(coll *mongo.Collection) (add func(bson.D), flush func()) {
		docs := make([]any, 0, *batch)
		flush = func() {
			if len(docs) == 0 {
				return
			}
			if _, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
				log.Fatalf("insert %s: %v", coll.Name(), err)
			}
			docs = docs[:0]
		}
		add = func(doc bson.D) {
			docs = append(docs, doc)
			if len(docs) == *batch {
				flush()
			}
		}
		return add, flush
	}

	add, flush := inserter(customers)
	customerIDs := make([]primitive.Binary, 0, *nCustomers)
	for i := 0; i < *nCustomers; i++ {
		doc, id := shopdata.Customer(r, i)
		customerIDs = append(customerIDs, id)
		add(doc)
	}
	flush()

	add, flush = inserter(products)
	skus := make([]string, 0, *nProducts)
	for i := 0; i < *nProducts; i++ {
		doc, sku := shopdata.Product(r, i)
		skus = append(skus, sku)
		add(doc)
	}
	flush()

	add, flush = inserter(orders)
	for i := 0; i < *nOrders; i++ {
		add(shopdata.Order(r, i, customerIDs, skus))
	}
	flush()

	for _, ix := range []struct {
		coll   *mongo.Collection
		models []mongo.IndexModel
	}{
		{customers, shopdata.CustomerIndexes()},
		{products, shopdata.ProductIndexes()},
		{orders, shopdata.OrderIndexes()},
	} {
		if _, err := ix.coll.Indexes().CreateMany(ctx, ix.models); err != nil {
			log.Fatalf("indexes on %s: %v", ix.coll.Name(), err)
		}
	}

	for _, c := range []*mongo.Collection{customers, products, orders} {
		report(ctx, db, c.Name())
	}
}

func report(ctx context.Context, db *mongo.Database, coll string) {
	var stats bson.M
	err := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: coll}}).Decode(&stats)
	if err != nil {
		log.Printf("%s: (collStats unavailable: %v)", coll, err)
		return
	}
	count, _ := toFloat(stats["count"])
	size, _ := toFloat(stats["size"])
	log.Printf("%s: %d docs, %.1f MB", coll, int(count), size/(1024*1024))
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
