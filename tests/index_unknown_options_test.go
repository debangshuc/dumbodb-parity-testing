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

func createIndexWithOptions(ctx context.Context, col *mongo.Collection, extra bson.D, flag any) (interface{}, error) {
	spec := bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: 1}}},
		{Key: "name", Value: "a_1"},
	}
	spec = append(spec, extra...)
	cmd := bson.D{
		{Key: "createIndexes", Value: col.Name()},
		{Key: "indexes", Value: bson.A{spec}},
	}
	if flag != nil {
		cmd = append(cmd, bson.E{Key: "ignoreUnknownIndexOptions", Value: flag})
	}
	if err := col.Database().RunCommand(ctx, cmd).Err(); err != nil {
		return nil, err
	}
	created, err := indexByName(ctx, col, "a_1")
	if err != nil {
		return nil, err
	}
	_, hasNS := created["ns"]
	_, hasBogus := created["bogus"]
	return bson.D{
		{Key: "created", Value: created != nil},
		{Key: "has_ns", Value: hasNS},
		{Key: "has_bogus", Value: hasBogus},
	}, nil
}

var legacyAndUnknownOptions = bson.D{
	{Key: "ns", Value: "restored.elsewhere"},
	{Key: "bogus", Value: true},
}

func TestIndex_IgnoreUnknownIndexOptions(t *testing.T) {
	cases := []struct {
		name  string
		extra bson.D
		flag  any
	}{
		{"Index_IgnoreUnknownIndexOptions_True", legacyAndUnknownOptions, true},
		{"Index_IgnoreUnknownIndexOptions_Int", legacyAndUnknownOptions, int32(1)},
		{"Index_IgnoreUnknownIndexOptions_Omitted", bson.D{{Key: "bogus", Value: true}}, nil},
		{"Index_IgnoreUnknownIndexOptions_False", bson.D{{Key: "bogus", Value: true}}, false},
		{"Index_IgnoreUnknownIndexOptions_WrongType", legacyAndUnknownOptions, "yes"},
		{"Index_IgnoreUnknownIndexOptions_KnownOptionStillValidated", bson.D{{Key: "unique", Value: "yes"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness.PairTest(t, harness.TestCase{
				Name:    tc.name,
				Support: harness.DumboDBFull,
				Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
					return createIndexWithOptions(ctx, col, tc.extra, tc.flag)
				},
			})
		})
	}
}

// MongoDB drops a legacy "ns" field from an index spec even without
// ignoreUnknownIndexOptions (index_key_validate.cpp); DumboDB rejects it.
func TestIndex_LegacyNsWithoutIgnoreFlag(t *testing.T) {
	harness.PairTest(t, harness.TestCase{
		Name:    "Index_LegacyNsWithoutIgnoreFlag",
		Support: harness.DumboDBXFail,
		Run: func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			return createIndexWithOptions(ctx, col, bson.D{{Key: "ns", Value: "restored.elsewhere"}}, nil)
		},
	})
}
