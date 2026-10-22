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
	"sort"
	"strconv"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// errOutcome reduces an error to the parts both servers must agree on.
func errOutcome(err error) bson.M {
	if err == nil {
		return bson.M{"ok": true}
	}
	code, name, _ := harness.CommandErrorCode(err)
	return bson.M{"ok": false, "code": code, "codeName": name}
}

func adminCommand(ctx context.Context, col *mongo.Collection, cmd bson.D) error {
	return col.Database().Client().Database("admin").RunCommand(ctx, cmd).Err()
}

func TestRename_DottedCollectionNames(t *testing.T) {
	harness.PairTest(t, harness.TestCase{
		Name:    "Rename_DottedCollectionNames",
		Support: harness.DumboDBFull,
		Setup: func(ctx context.Context, col *mongo.Collection) error {
			_, err := col.Database().Collection("a.b").InsertOne(ctx, bson.D{{Key: "_id", Value: 1}})
			return err
		},
		Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			db := col.Database().Name()
			out := bson.M{
				"rename dotted source": errOutcome(adminCommand(ctx, col, bson.D{{Key: "renameCollection", Value: db + ".a.b"}, {Key: "to", Value: db + ".c"}})),
				"rename dotted target": errOutcome(adminCommand(ctx, col, bson.D{{Key: "renameCollection", Value: db + ".c"}, {Key: "to", Value: db + ".d.e"}})),
				"dataSize dotted":      errOutcome(col.Database().RunCommand(ctx, bson.D{{Key: "dataSize", Value: db + ".d.e"}}).Err()),
			}
			names, err := col.Database().ListCollectionNames(ctx, bson.D{})
			if err != nil {
				return nil, err
			}
			sort.Strings(names)
			out["collections"] = names
			return out, nil
		},
	})
}

func TestRename_SystemCollectionRules(t *testing.T) {
	harness.PairTest(t, harness.TestCase{
		Name:    "Rename_SystemCollectionRules",
		Support: harness.DumboDBFull,
		Setup: func(ctx context.Context, col *mongo.Collection) error {
			_, err := col.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}})
			return err
		},
		Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			src := col.Database().Name() + "." + col.Name()
			out := bson.M{}
			for label, to := range map[string]string{
				"to system.users": "admin.system.users",
				"to system.js":    col.Database().Name() + ".system.js",
				"to system.views": col.Database().Name() + ".system.views",
				"to other system": col.Database().Name() + ".system.other",
				"no collection":   col.Database().Name() + ".",
				"no database":     ".x",
				"not a namespace": "x",
			} {
				out[label] = errOutcome(adminCommand(ctx, col, bson.D{{Key: "renameCollection", Value: src}, {Key: "to", Value: to}}))
			}
			out["from system.users"] = errOutcome(adminCommand(ctx, col, bson.D{{Key: "renameCollection", Value: "admin.system.users"}, {Key: "to", Value: "admin.users_copy"}}))
			return out, nil
		},
	})
}

func TestDatabase_NamesDifferingOnlyInCase(t *testing.T) {
	harness.PairTest(t, harness.TestCase{
		Name:    "Database_NamesDifferingOnlyInCase",
		Support: harness.DumboDBFull,
		Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			client := col.Database().Client()
			suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
			base := "Case_" + suffix
			lower := "case_" + suffix
			defer func() {
				_ = client.Database(base).Drop(ctx)
				_ = client.Database(lower).Drop(ctx)
			}()
			_, err := client.Database(base).Collection("c").InsertOne(ctx, bson.D{{Key: "a", Value: 1}})
			if err != nil {
				return nil, err
			}
			_, err = client.Database(lower).Collection("c").InsertOne(ctx, bson.D{{Key: "a", Value: 1}})
			var found bson.M
			readErr := client.Database(lower).Collection("c").FindOne(ctx, bson.D{}).Decode(&found)
			return bson.M{"insert": errOutcome(err), "find": errOutcome(readErr)}, nil
		},
	})
}

// MongoDB lets privileged users create and rename collections in admin.
// DumboDB reserves admin for its user and role data and refuses both.
func TestRename_WithinAdminDatabase(t *testing.T) {
	harness.PairTest(t, harness.TestCase{
		Name:    "Rename_WithinAdminDatabase",
		Support: harness.DumboDBXFail,
		Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			admin := col.Database().Client().Database("admin")
			suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
			from, to := "parity_from_"+suffix, "parity_to_"+suffix
			defer func() {
				_ = admin.Collection(from).Drop(ctx)
				_ = admin.Collection(to).Drop(ctx)
			}()
			_, insertErr := admin.Collection(from).InsertOne(ctx, bson.D{{Key: "a", Value: 1}})
			renameErr := admin.RunCommand(ctx, bson.D{{Key: "renameCollection", Value: "admin." + from}, {Key: "to", Value: "admin." + to}}).Err()
			return bson.M{"insert": errOutcome(insertErr), "rename": errOutcome(renameErr)}, nil
		},
	})
}
