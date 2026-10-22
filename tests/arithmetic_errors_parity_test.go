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
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// Arithmetic errors: MongoDB reports division and modulo by zero, and
// non-numeric operands, with specific codes (BadValue, TypeMismatch,
// Location16610, ...). DumboDB reports most of them as InternalError or
// Location16020 (workspace-1du), and some math operators return wrong values
// (workspace-2ms). Each case is its own pair test so it can move to
// DumboDBFull as it is fixed.

func insertArithmeticDocs(ctx context.Context, col *mongo.Collection) error {
	docs := make([]interface{}, 300)
	for i := range docs {
		d := 1
		if i == 0 || i == 250 {
			d = 0
		}
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i + 7}, {Key: "d", Value: d}, {Key: "s", Value: "x"}}
	}
	_, err := col.InsertMany(ctx, docs)
	return err
}

func drainCursor(ctx context.Context, cursor *mongo.Cursor, err error) (interface{}, error) {
	if err != nil {
		return errOutcome(err), nil
	}
	var docs []bson.M
	if err = cursor.All(ctx, &docs); err != nil {
		return errOutcome(err), nil
	}
	return docs, nil
}

// projectFirst evaluates expr against the first document only.
func projectFirst(expr interface{}) func(context.Context, *mongo.Collection) (interface{}, error) {
	return func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
		cursor, err := col.Aggregate(ctx, bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: 0}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "r", Value: expr}}}},
		})
		return drainCursor(ctx, cursor, err)
	}
}

// projectTyped is projectFirst returning the result's BSON type with its value,
// so an int/long/double difference is not compared away.
func projectTyped(expr interface{}) func(context.Context, *mongo.Collection) (interface{}, error) {
	return func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
		cursor, err := col.Aggregate(ctx, bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: 0}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "r", Value: expr}}}},
		})
		if err != nil {
			return errOutcome(err), nil
		}
		var docs []bson.Raw
		if err = cursor.All(ctx, &docs); err != nil {
			return errOutcome(err), nil
		}
		value := docs[0].Lookup("r")
		return bson.M{"type": value.Type.String(), "value": value.String()}, nil
	}
}

func pipelineRun(pipeline bson.A) func(context.Context, *mongo.Collection) (interface{}, error) {
	return func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
		cursor, err := col.Aggregate(ctx, pipeline)
		return drainCursor(ctx, cursor, err)
	}
}

func TestArithmeticErrors(t *testing.T) {
	dec0, err := primitive.ParseDecimal128("0")
	if err != nil {
		t.Fatal(err)
	}
	decNegOne, err := primitive.ParseDecimal128("-1")
	if err != nil {
		t.Fatal(err)
	}
	op := func(name string, arg interface{}) bson.D { return bson.D{{Key: name, Value: arg}} }
	divide := func(a, b interface{}) bson.D { return bson.D{{Key: "$divide", Value: bson.A{a, b}}} }
	mod := func(a, b interface{}) bson.D { return bson.D{{Key: "$mod", Value: bson.A{a, b}}} }

	cases := []struct {
		name    string
		support harness.DumboDBSupport
		run     func(context.Context, *mongo.Collection) (interface{}, error)
	}{
		{"Divide_ZeroField", harness.DumboDBXFail, projectFirst(divide("$v", "$d"))},
		{"Divide_ZeroInt", harness.DumboDBXFail, projectFirst(divide("$v", 0))},
		{"Divide_ZeroLong", harness.DumboDBXFail, projectFirst(divide("$v", int64(0)))},
		{"Divide_ZeroDouble", harness.DumboDBXFail, projectFirst(divide("$v", 0.0))},
		{"Divide_NegativeZeroDouble", harness.DumboDBXFail, projectFirst(divide("$v", -0.0))},
		{"Divide_ZeroDecimal", harness.DumboDBXFail, projectFirst(divide("$v", dec0))},
		{"Divide_ZeroByZeroDouble", harness.DumboDBXFail, projectFirst(divide(0.0, 0.0))},
		{"Divide_StringDivisor", harness.DumboDBXFail, projectFirst(divide("$v", "$s"))},
		{"Divide_NullDivisor", harness.DumboDBFull, projectFirst(divide("$v", nil))},
		{"Divide_MissingDivisor", harness.DumboDBFull, projectFirst(divide("$v", "$missing"))},
		{"Mod_ZeroInt", harness.DumboDBXFail, projectFirst(mod("$v", 0))},
		{"Mod_ZeroLong", harness.DumboDBXFail, projectFirst(mod("$v", int64(0)))},
		{"Mod_ZeroDouble", harness.DumboDBXFail, projectFirst(mod("$v", 0.0))},
		{"Mod_NegativeZeroDouble", harness.DumboDBXFail, projectFirst(mod("$v", -0.0))},
		{"Mod_ZeroDecimal", harness.DumboDBXFail, projectFirst(mod("$v", dec0))},
		{"Mod_StringDivisor", harness.DumboDBXFail, projectFirst(mod("$v", "$s"))},
		{"Mod_NullDivisor", harness.DumboDBFull, projectFirst(mod("$v", nil))},
		{"Divide_ZeroAfterFirstBatch", harness.DumboDBXFail, pipelineRun(bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: 1}}}}}},
			bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
			bson.D{{Key: "$project", Value: bson.D{{Key: "r", Value: divide("$v", "$d")}}}},
		})},
		{"Divide_InAddFields", harness.DumboDBXFail, pipelineRun(bson.A{
			bson.D{{Key: "$addFields", Value: bson.D{{Key: "r", Value: divide("$v", 0)}}}},
		})},
		{"Divide_InGroupAccumulator", harness.DumboDBXFail, pipelineRun(bson.A{
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "r", Value: bson.D{{Key: "$sum", Value: divide("$v", 0)}}}}}},
		})},
		{"Divide_InFindExpr", harness.DumboDBXFail, func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			cursor, err := col.Find(ctx, bson.D{{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{divide("$v", "$d"), 0}}}}})
			return drainCursor(ctx, cursor, err)
		}},
		{"Divide_InUpdatePipeline", harness.DumboDBXFail, func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			_, err := col.UpdateOne(ctx, bson.D{{Key: "_id", Value: 0}}, bson.A{bson.D{{Key: "$set", Value: bson.D{{Key: "r", Value: divide("$v", 0)}}}}})
			return errOutcome(err), nil
		}},
		{"Mod_QueryOperatorZeroDivisor", harness.DumboDBFull, func(ctx context.Context, col *mongo.Collection) (interface{}, error) {
			cursor, err := col.Find(ctx, bson.D{{Key: "v", Value: bson.D{{Key: "$mod", Value: bson.A{0, 0}}}}})
			return drainCursor(ctx, cursor, err)
		}},

		// Math operators (workspace-2ms): DumboDB returns a value, often wrong,
		// where MongoDB errors or computes a different value.
		{"Math_SubtractLongOverflow", harness.DumboDBXFail, projectTyped(op("$subtract", bson.A{int64(math.MinInt64), int64(1)}))},
		{"Math_AbsMinInt", harness.DumboDBXFail, projectTyped(op("$abs", int32(math.MinInt32)))},
		{"Math_AbsMinLong", harness.DumboDBXFail, projectTyped(op("$abs", int64(math.MinInt64)))},
		{"Math_SqrtString", harness.DumboDBXFail, projectTyped(op("$sqrt", "x"))},
		{"Math_PowString", harness.DumboDBXFail, projectTyped(op("$pow", bson.A{"x", 2}))},
		{"Math_TruncString", harness.DumboDBXFail, projectTyped(op("$trunc", "x"))},
		{"Math_LogString", harness.DumboDBXFail, projectTyped(op("$log", bson.A{"x", 10}))},
		{"Math_SqrtNegative", harness.DumboDBXFail, projectTyped(op("$sqrt", -1))},
		{"Math_SqrtNegativeDecimal", harness.DumboDBXFail, projectTyped(op("$sqrt", decNegOne))},
		{"Math_LnZero", harness.DumboDBXFail, projectTyped(op("$ln", 0))},
		{"Math_LnNegative", harness.DumboDBXFail, projectTyped(op("$ln", -1))},
		{"Math_LnNegativeDecimal", harness.DumboDBXFail, projectTyped(op("$ln", decNegOne))},
		{"Math_LogZero", harness.DumboDBXFail, projectTyped(op("$log", bson.A{0, 10}))},
		{"Math_LogNegative", harness.DumboDBXFail, projectTyped(op("$log", bson.A{-1, 10}))},
		{"Math_LogBaseOne", harness.DumboDBXFail, projectTyped(op("$log", bson.A{10, 1}))},
		{"Math_LogBaseNegative", harness.DumboDBXFail, projectTyped(op("$log", bson.A{10, -2}))},
		{"Math_Log10Zero", harness.DumboDBXFail, projectTyped(op("$log10", 0))},
		{"Math_Log10Negative", harness.DumboDBXFail, projectTyped(op("$log10", -1))},
		{"Math_PowZeroNegativeExponent", harness.DumboDBXFail, projectTyped(op("$pow", bson.A{0, -1}))},
		{"Math_PowZeroDoubleNegativeExponent", harness.DumboDBXFail, projectTyped(op("$pow", bson.A{0.0, -1}))},
		{"Math_PowIntBeyondInt32", harness.DumboDBXFail, projectTyped(op("$pow", bson.A{2, 40}))},
		{"Math_RoundPlaceOutOfRange", harness.DumboDBXFail, projectTyped(op("$round", bson.A{1.5, 200}))},
		{"Math_AddIntOverflow", harness.DumboDBFull, projectTyped(op("$add", bson.A{int32(math.MaxInt32), int32(1)}))},
		{"Math_AddLongOverflow", harness.DumboDBFull, projectTyped(op("$add", bson.A{int64(math.MaxInt64), int64(1)}))},
		{"Math_MultiplyIntOverflow", harness.DumboDBFull, projectTyped(op("$multiply", bson.A{int32(math.MaxInt32), int32(2)}))},
		{"Math_MultiplyLongOverflow", harness.DumboDBFull, projectTyped(op("$multiply", bson.A{int64(math.MaxInt64), int64(2)}))},
		{"Math_PowLongOverflow", harness.DumboDBFull, projectTyped(op("$pow", bson.A{int64(2), 70}))},
		{"Math_ExpOverflow", harness.DumboDBFull, projectTyped(op("$exp", 1000))},
	}

	for _, tc := range cases {
		harness.PairTest(t, harness.TestCase{
			Name:    "Arithmetic_" + tc.name,
			Support: tc.support,
			Setup:   insertArithmeticDocs,
			Run:     tc.run,
		})
	}
}
