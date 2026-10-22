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
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/dolthub/dumbodb-parity-testing/harness"
	"github.com/dolthub/dumbodb-parity-testing/wire"
)

// replyOutcome keeps the parts of a command reply both servers must agree on;
// error messages may differ.
func replyOutcome(reply bson.M) bson.M {
	return bson.M{"ok": reply["ok"], "code": reply["code"], "codeName": reply["codeName"]}
}

func dialWire(addr string) (*wire.Conn, error) {
	c, err := wire.Dial(addr)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	return c, nil
}

func TestWire_OpMsgWithoutBody(t *testing.T) {
	runWireParity(t, "opmsg_without_body", harness.DumboDBFull, harness.TopologyStandalone, func(addr, dbName string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		doc, err := bson.Marshal(bson.D{{Key: "ping", Value: 1}, {Key: "$db", Value: dbName}})
		if err != nil {
			return nil, err
		}
		section := []byte{1}
		section = binary.LittleEndian.AppendUint32(section, uint32(4+len("docs")+1+len(doc)))
		section = append(section, "docs\x00"...)
		section = append(section, doc...)
		reply, err := c.RunRawOpMsg(append(make([]byte, 4), section...))
		if err != nil {
			return nil, err
		}
		return replyOutcome(reply), nil
	})
}

func TestWire_OpMsgInvalidBSONBody(t *testing.T) {
	runWireParity(t, "opmsg_invalid_bson_body", harness.DumboDBFull, harness.TopologyStandalone, func(addr, _ string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		// {ping: <element of unknown type 0x7f>}
		doc := []byte{0, 0, 0, 0, 0x7f, 'p', 'i', 'n', 'g', 0, 1, 0}
		binary.LittleEndian.PutUint32(doc, uint32(len(doc)))
		reply, err := c.RunRawOpMsg(append(make([]byte, 5), doc...))
		if err != nil {
			return nil, err
		}
		return replyOutcome(reply), nil
	})
}

var databaseNameProbeCommands = []bson.D{
	{{Key: "ping", Value: 1}},
	{{Key: "find", Value: "c"}},
	{{Key: "insert", Value: "c"}, {Key: "documents", Value: bson.A{bson.D{{Key: "a", Value: 1}}}}},
	{{Key: "createUser", Value: "u"}, {Key: "pwd", Value: "pw"}, {Key: "roles", Value: bson.A{}}},
	{{Key: "usersInfo", Value: 1}},
	{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: []byte("n,,n=u,r=abcdefghijklmnopqrstuvwx")}},
}

func probeDatabaseNames(addr, dbName string, suffixes []string) (interface{}, error) {
	c, err := dialWire(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	out := bson.M{}
	for _, suffix := range suffixes {
		for _, cmd := range databaseNameProbeCommands {
			reply, err := c.RunCommand(append(cmd, bson.E{Key: "$db", Value: dbName + suffix}))
			if err != nil {
				return nil, err
			}
			out[fmt.Sprintf("%q %s", suffix, cmd[0].Key)] = replyOutcome(reply)
		}
	}
	return out, nil
}

func TestWire_InvalidDatabaseNames(t *testing.T) {
	runWireParity(t, "invalid_database_names", harness.DumboDBFull, harness.TopologyStandalone, func(addr, dbName string) (interface{}, error) {
		return probeDatabaseNames(addr, dbName, []string{".x", " x", "/x", `"x`})
	})
}

// MongoDB rejects '$' only for commands that would create the database.
// DumboDB rejects it for reads too (workspace-ax6).
func TestWire_DollarInDatabaseName(t *testing.T) {
	runWireParity(t, "dollar_in_database_name", harness.DumboDBXFail, harness.TopologyStandalone, func(addr, dbName string) (interface{}, error) {
		return probeDatabaseNames(addr, dbName, []string{"$x"})
	})
}

func TestWire_RoleReferenceInvalidDatabaseName(t *testing.T) {
	runWireParity(t, "role_reference_invalid_database_name", harness.DumboDBFull, harness.TopologyStandalone, func(addr, dbName string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		reply, err := c.RunCommand(bson.D{
			{Key: "createUser", Value: "u"},
			{Key: "pwd", Value: "pw"},
			{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: dbName + ".x"}}}},
			{Key: "$db", Value: dbName},
		})
		if err != nil {
			return nil, err
		}
		return replyOutcome(reply), nil
	})
}

func TestWire_SASLprepBidiPassword(t *testing.T) {
	runWireParity(t, "saslprep_bidi_password", harness.DumboDBFull, harness.TopologyStandalone, func(addr, dbName string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		reply, err := c.RunCommand(bson.D{
			{Key: "createUser", Value: "bidi"},
			{Key: "pwd", Value: "plaintextsecret\u05d0"},
			{Key: "roles", Value: bson.A{}},
			{Key: "$db", Value: dbName},
		})
		if err != nil {
			return nil, err
		}
		return bson.M{"outcome": replyOutcome(reply), "errmsg": reply["errmsg"]}, nil
	})
}

func TestWire_DebugErrorIsNotACommand(t *testing.T) {
	runWireParity(t, "debug_error_not_a_command", harness.DumboDBFull, harness.TopologyStandalone, func(addr, _ string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		reply, err := c.RunCommand(bson.D{{Key: "debugError", Value: "panic"}, {Key: "$db", Value: "admin"}})
		if err != nil {
			return nil, err
		}
		return replyOutcome(reply), nil
	})
}

// An awaitable hello with maxAwaitTimeMS:0 must not stream exhaust responses
// as fast as the client reads them. DumboDB serves awaitable hello only with
// --replSet and the suite runs it standalone, so the DumboDB side diverges
// (workspace-nwt).
func TestWire_ExhaustHelloIsPaced(t *testing.T) {
	runWireParity(t, "exhaust_hello_paced", harness.DumboDBXFail, harness.TopologyStandalone, func(addr, _ string) (interface{}, error) {
		c, err := dialWire(addr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		hello, err := c.RunCommand(bson.D{{Key: "hello", Value: 1}, {Key: "$db", Value: "admin"}})
		if err != nil {
			return nil, err
		}
		if hello["topologyVersion"] == nil {
			return nil, fmt.Errorf("hello has no topologyVersion")
		}
		if _, _, err = c.RunCommandFlags(bson.D{
			{Key: "hello", Value: 1},
			{Key: "topologyVersion", Value: hello["topologyVersion"]},
			{Key: "maxAwaitTimeMS", Value: int64(0)},
			{Key: "$db", Value: "admin"},
		}, wire.FlagExhaustAllowed); err != nil {
			return nil, err
		}
		responses := 1
		_ = c.SetDeadline(time.Now().Add(1200 * time.Millisecond))
		for {
			if _, _, err := c.ReadReply(); err != nil {
				break
			}
			responses++
		}
		return bson.M{"paced": responses <= 4}, nil
	})
}
