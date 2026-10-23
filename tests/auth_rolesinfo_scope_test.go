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
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/dolthub/dumbodb-parity-testing/harness"
)

// rolesInfo needs viewRole on the database of each role it names, except
// roles granted to the caller directly; rolesInfo: 1 needs viewRole on $db.
func TestAuthRolesInfoChecksEachRoleDatabase(t *testing.T) {
	tc := authCaseFull("ROLESINFO-cross-db", func(ctx context.Context, tgt harness.AuthTarget) (interface{}, error) {
		home, other := "rih_"+tgt.NS, "rio_"+tgt.NS
		admin, reader := "ra_"+tgt.NS, "rr_"+tgt.NS
		defer func() {
			cleanupUser(ctx, tgt, home, admin)
			cleanupUser(ctx, tgt, home, reader)
			_ = harness.DropRole(ctx, tgt.Admin, other, "secret")
			_ = tgt.Admin.Database(home).Drop(ctx)
			_ = tgt.Admin.Database(other).Drop(ctx)
		}()
		tgt.Setup(harness.CreateRole(ctx, tgt.Admin, other, "secret", nil, nil))
		tgt.Setup(harness.CreateUser(ctx, tgt.Admin, home, admin, "pw", []harness.RoleRef{{Role: "userAdmin", DB: home}}))
		tgt.Setup(harness.CreateUser(ctx, tgt.Admin, home, reader, "pw", []harness.RoleRef{{Role: "read", DB: home}}))

		asAdmin, err := harness.ConnectAs(ctx, tgt.BaseURI, admin, "pw", home)
		tgt.Setup(err)
		defer func() { _ = asAdmin.Disconnect(ctx) }()
		asReader, err := harness.ConnectAs(ctx, tgt.BaseURI, reader, "pw", home)
		tgt.Setup(err)
		defer func() { _ = asReader.Disconnect(ctx) }()

		ref := func(role, db string) bson.D { return bson.D{{Key: "role", Value: role}, {Key: "db", Value: db}} }
		out := bson.M{}
		for label, tc := range map[string]struct {
			admin bool
			arg   interface{}
		}{
			"admin own db all":        {true, 1},
			"admin own db named":      {true, ref("userAdmin", home)},
			"admin other db role":     {true, ref("secret", other)},
			"admin mixed":             {true, bson.A{ref("userAdmin", home), ref("secret", other)}},
			"admin other db all":      {true, 1},
			"reader own granted role": {false, ref("read", home)},
			"reader ungranted role":   {false, ref("readWrite", home)},
			"reader own db all":       {false, 1},
		} {
			client := asReader
			if tc.admin {
				client = asAdmin
			}
			db := home
			if label == "admin other db all" {
				db = other
			}
			out[label] = errOutcome(client.Database(db).RunCommand(ctx, bson.D{{Key: "rolesInfo", Value: tc.arg}}).Err())
		}
		allowed, denied := 0, 0
		for _, v := range out {
			if v.(bson.M)["ok"] == true {
				allowed++
			} else if v.(bson.M)["code"] == int32(13) {
				denied++
			}
		}
		if allowed == 0 || denied == 0 {
			tgt.Setup(fmt.Errorf("outcomes are not a mix of allowed and Unauthorized: %v", out))
		}
		return out, nil
	})
	harness.AuthPairTest(t, tc)
}
