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
	"strings"
	"testing"
	"time"

	"github.com/dolthub/dumbodb-parity-testing/harness"
	"github.com/dolthub/dumbodb-parity-testing/wire"
	"go.mongodb.org/mongo-driver/bson"
)

// mongod accepts OP_COMPRESSED for every compressor it has enabled, whether or
// not the client negotiated it, and answers a compressed request with a reply
// compressed the same way; noop always closes the connection. DumboDB enables
// only snappy so far (dolthub/dumbodb#109), so cases using zlib are XFail.
func TestCompression_wire(t *testing.T) {
	const plain = -1
	type step struct {
		command    bson.D
		compressor int
	}
	ping := bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}}
	helloSnappy := bson.D{{Key: "hello", Value: int32(1)}, {Key: "compression", Value: bson.A{"snappy"}}, {Key: "$db", Value: "admin"}}
	negotiate := step{helloSnappy, plain}

	cases := []struct {
		name    string
		steps   []step
		support harness.DumboDBSupport
	}{
		{"snappy_negotiated", []step{negotiate, {ping, int(wire.CompressorSnappy)}}, harness.DumboDBFull},
		{"snappy_unsolicited", []step{{ping, int(wire.CompressorSnappy)}}, harness.DumboDBFull},
		{"plain_after_negotiation", []step{negotiate, {ping, plain}}, harness.DumboDBFull},
		{"mixed_stream", []step{negotiate, {ping, int(wire.CompressorSnappy)}, {ping, plain}, {ping, int(wire.CompressorSnappy)}}, harness.DumboDBFull},
		{"snappy_hello", []step{{helloSnappy, int(wire.CompressorSnappy)}}, harness.DumboDBFull},
		{"noop_refused", []step{negotiate, {ping, int(wire.CompressorNoop)}}, harness.DumboDBFull},
		{"zlib_unsolicited", []step{{ping, int(wire.CompressorZlib)}}, harness.DumboDBXFail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWireParity(t, "compression_"+tc.name, tc.support, harness.TopologyStandalone,
				func(addr, _ string) (interface{}, error) {
					c, err := wire.Dial(addr)
					if err != nil {
						return nil, fmt.Errorf("dial: %w", err)
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(15 * time.Second))

					var replies bson.A
					for _, s := range tc.steps {
						var reply bson.M
						if s.compressor == plain {
							reply, err = c.RunCommand(s.command)
						} else {
							reply, err = c.RunCompressedCommand(s.command, byte(s.compressor))
						}
						if err != nil {
							replies = append(replies, bson.D{{Key: "closed", Value: connectionClosed(err)}})
							break
						}
						replies = append(replies, bson.D{
							{Key: "ok", Value: reply["ok"]},
							{Key: "replyCompressor", Value: c.LastReplyCompressor()},
						})
					}
					return replies, nil
				})
		})
	}
}

// TestCompression_wire_snappy_roundtrip writes and reads back a large document
// over snappy, so the compressed path carries real data, not just ping.
func TestCompression_wire_snappy_roundtrip(t *testing.T) {
	runWireParity(t, "compression_roundtrip", harness.DumboDBFull, harness.TopologyStandalone,
		func(addr, dbName string) (interface{}, error) {
			c, err := wire.Dial(addr)
			if err != nil {
				return nil, fmt.Errorf("dial: %w", err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(15 * time.Second))

			doc := bson.D{{Key: "_id", Value: "big"}, {Key: "text", Value: strings.Repeat("compressible order line ", 20000)}}
			insert, err := c.RunCompressedCommand(bson.D{
				{Key: "insert", Value: "col"},
				{Key: "documents", Value: bson.A{doc}},
				{Key: "$db", Value: dbName},
			}, wire.CompressorSnappy)
			if err != nil {
				return nil, fmt.Errorf("insert: %w", err)
			}
			insertCompressor := c.LastReplyCompressor()
			found, err := c.RunCompressedCommand(bson.D{
				{Key: "find", Value: "col"},
				{Key: "filter", Value: bson.D{{Key: "_id", Value: "big"}}},
				{Key: "$db", Value: dbName},
			}, wire.CompressorSnappy)
			if err != nil {
				return nil, fmt.Errorf("find: %w", err)
			}
			cursor, _ := found["cursor"].(bson.M)
			batch, _ := cursor["firstBatch"].(bson.A)
			var text string
			if len(batch) == 1 {
				text, _ = batch[0].(bson.M)["text"].(string)
			}
			return bson.D{
				{Key: "inserted", Value: insert["n"]},
				{Key: "insertReplyCompressor", Value: insertCompressor},
				{Key: "found", Value: len(batch)},
				{Key: "textIntact", Value: text == doc[1].Value},
				{Key: "findReplyCompressor", Value: c.LastReplyCompressor()},
			}, nil
		})
}

// connectionClosed reports whether err is the server closing the connection
// rather than some other failure, which the comparison should surface as is.
func connectionClosed(err error) interface{} {
	message := err.Error()
	for _, closed := range []string{"EOF", "connection reset", "broken pipe"} {
		if strings.Contains(message, closed) {
			return true
		}
	}
	return message
}
