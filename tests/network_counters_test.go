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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/dumbodb-parity-testing/harness"
	"github.com/dolthub/dumbodb-parity-testing/wire"
	"go.mongodb.org/mongo-driver/bson"
)

// TestServerStatusNetworkCounters checks serverStatus.network against what a
// connection actually sent and received. The counters are server-wide, so the
// test owns a private server pair: any other client would move them. Each server
// reports, per counter, the change minus the traffic tallied on the wire; mongod
// reports all zeros, and DumboDB must too.
func TestServerStatusNetworkCounters(t *testing.T) {
	servers := harness.StartRestartableServers(t)
	mongo, err := networkCounterResiduals(servers.MongoURI)
	if err != nil {
		t.Fatalf("mongod: %v", err)
	}
	for _, e := range mongo {
		if e.Value != int64(0) {
			t.Fatalf("mongod %s is off by %v; the test's accounting no longer matches mongod", e.Key, e.Value)
		}
	}
	dumbo, err := networkCounterResiduals(servers.DumboDBURI)
	if err != nil {
		t.Fatalf("dumbodb: %v", err)
	}
	cmp := harness.CompareResponses(mongo, nil, dumbo, nil)
	if cmp.Result != harness.Match {
		t.Errorf("FULL network_counters: DIVERGE\n%s", cmp.Diff)
	}
}

var networkCounterPaths = []string{
	"bytesIn", "bytesOut", "physicalBytesIn", "physicalBytesOut", "numRequests",
	"compression.snappy.compressor.bytesIn", "compression.snappy.compressor.bytesOut",
	"compression.snappy.decompressor.bytesIn", "compression.snappy.decompressor.bytesOut",
}

func networkCounterResiduals(uri string) (bson.D, error) {
	reader, err := wire.Dial(uri)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	traffic, err := wire.Dial(uri)
	if err != nil {
		return nil, err
	}
	defer traffic.Close()
	deadline := time.Now().Add(30 * time.Second)
	_ = reader.SetDeadline(deadline)
	_ = traffic.SetDeadline(deadline)

	read := func() (map[string]int64, error) {
		reply, err := reader.RunCommand(bson.D{{Key: "serverStatus", Value: int32(1)}, {Key: "$db", Value: "admin"}})
		if err != nil {
			return nil, err
		}
		counters := map[string]int64{}
		for _, path := range networkCounterPaths {
			var value interface{} = reply["network"]
			for _, key := range strings.Split(path, ".") {
				document, _ := value.(bson.M)
				value = document[key]
			}
			number, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("serverStatus.network.%s is %T, want int64", path, value)
			}
			counters[path] = number
		}
		return counters, nil
	}

	before, err := read()
	if err != nil {
		return nil, err
	}
	readerBefore := reader.Traffic()

	r := rand.New(rand.NewSource(1))
	for i := 0; i < 40; i++ {
		var comment strings.Builder
		for j := 0; j < 10000; j++ {
			comment.WriteByte(byte('a' + r.Intn(4)))
		}
		ping := bson.D{{Key: "ping", Value: int32(1)}, {Key: "comment", Value: comment.String()}, {Key: "$db", Value: "admin"}}
		if i%2 == 0 {
			_, err = traffic.RunCompressedCommand(ping, wire.CompressorSnappy)
		} else {
			_, err = traffic.RunCommand(ping)
		}
		if err != nil {
			return nil, fmt.Errorf("ping %d: %w", i, err)
		}
	}

	after, err := read()
	if err != nil {
		return nil, err
	}
	// The second reading counts its own request but not its reply, and counts
	// the first reading's reply.
	readerAfter := reader.Traffic()
	readRequestLogical := readerAfter.RequestLogicalBytes - readerBefore.RequestLogicalBytes
	readRequestWire := readerAfter.RequestWireBytes - readerBefore.RequestWireBytes
	firstReplyLogical := readerBefore.ReplyLogicalBytes
	firstReplyWire := readerBefore.ReplyWireBytes
	sent := traffic.Traffic()

	expected := map[string]int64{
		"bytesIn":          sent.RequestLogicalBytes + readRequestLogical,
		"bytesOut":         sent.ReplyLogicalBytes + firstReplyLogical,
		"physicalBytesIn":  sent.RequestWireBytes + readRequestWire,
		"physicalBytesOut": sent.ReplyWireBytes + firstReplyWire,
		"numRequests":      sent.Requests + 1,

		"compression.snappy.compressor.bytesIn":    sent.ReplyBodyBytes,
		"compression.snappy.compressor.bytesOut":   sent.ReplyPayloadBytes,
		"compression.snappy.decompressor.bytesIn":  sent.RequestPayloadBytes,
		"compression.snappy.decompressor.bytesOut": sent.RequestBodyBytes,
	}
	var residuals bson.D
	for _, path := range networkCounterPaths {
		residuals = append(residuals, bson.E{Key: path, Value: after[path] - before[path] - expected[path]})
	}
	return residuals, nil
}
