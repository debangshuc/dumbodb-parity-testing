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
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/dumbodb-parity-testing/harness"
	"github.com/dolthub/dumbodb-parity-testing/wire"
	"go.mongodb.org/mongo-driver/bson"
)

// Compression is negotiated in the connection handshake: the client lists the
// compressors it supports in hello, and the server replies with the ones it
// accepts. The Go driver hides that exchange, so these tests send the handshake
// over OP_MSG directly. Each case uses a fresh connection so no case inherits
// another's negotiation; mongod also negotiates on a later hello (second_hello).
//
// DumboDB supports only snappy so far (dolthub/dumbodb#109), so cases where
// mongod also accepts zlib or zstd are XFail until DumboDB supports them.
func TestHelloCompression_wire(t *testing.T) {
	cases := []struct {
		name        string
		command     string
		compression bson.A
		priorHello  bool
		support     harness.DumboDBSupport
	}{
		{"hello_all", "hello", bson.A{"snappy", "zlib", "zstd"}, false, harness.DumboDBXFail},
		{"hello_subset", "hello", bson.A{"zstd"}, false, harness.DumboDBXFail},
		{"hello_client_order", "hello", bson.A{"zstd", "zlib", "snappy"}, false, harness.DumboDBXFail},
		{"hello_with_unknown", "hello", bson.A{"lz4", "zlib"}, false, harness.DumboDBXFail},
		{"hello_only_unknown", "hello", bson.A{"lz4"}, false, harness.DumboDBFull},
		{"hello_snappy", "hello", bson.A{"snappy"}, false, harness.DumboDBFull},
		{"hello_snappy_with_unknown", "hello", bson.A{"lz4", "snappy"}, false, harness.DumboDBFull},
		{"ismaster_snappy", "isMaster", bson.A{"snappy"}, false, harness.DumboDBFull},
		{"second_hello_snappy", "hello", bson.A{"snappy"}, true, harness.DumboDBFull},
		{"hello_empty", "hello", bson.A{}, false, harness.DumboDBFull},
		{"ismaster_all", "isMaster", bson.A{"snappy", "zlib", "zstd"}, false, harness.DumboDBXFail},
		{"second_hello", "hello", bson.A{"snappy", "zlib", "zstd"}, true, harness.DumboDBXFail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWireParity(t, "hello_"+tc.name, tc.support, harness.TopologyStandalone,
				func(addr, _ string) (interface{}, error) {
					return helloCompressionReply(addr, tc.command, tc.compression, tc.priorHello)
				})
		})
	}
}

// helloCompressionReply returns only the reply fields that negotiation decides;
// the rest of a hello reply is server- and connection-specific.
func helloCompressionReply(addr, command string, compression bson.A, priorHello bool) (interface{}, error) {
	c, err := wire.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))

	if priorHello {
		if _, err := c.RunCommand(bson.D{{Key: "hello", Value: int32(1)}, {Key: "$db", Value: "admin"}}); err != nil {
			return nil, fmt.Errorf("prior hello: %w", err)
		}
	}
	reply, err := c.RunCommand(bson.D{
		{Key: command, Value: int32(1)},
		{Key: "compression", Value: compression},
		{Key: "$db", Value: "admin"},
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", command, err)
	}
	negotiated, advertised := reply["compression"]
	return bson.D{
		{Key: "ok", Value: reply["ok"]},
		{Key: "advertised", Value: advertised},
		{Key: "compression", Value: negotiated},
	}, nil
}
