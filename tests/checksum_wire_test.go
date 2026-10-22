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

// An OP_MSG may end in a CRC-32C of the whole message when its checksumPresent
// flag is set; drivers rarely send one, so these tests build the frames by hand.
// Both servers accept a correct checksum and close the connection on a wrong
// one, plain or snappy-compressed. mongod also checksums its reply to a
// checksummed request, which DumboDB does not yet do, so those cases are XFail.
func TestChecksum_wire(t *testing.T) {
	const plain = -1
	snappy := int(wire.CompressorSnappy)
	cases := []struct {
		name        string
		checksum    wire.Checksum
		compressor  int
		replyDetail bool
		support     harness.DumboDBSupport
	}{
		{"valid_accepted", wire.ChecksumValid, plain, false, harness.DumboDBFull},
		{"corrupt_closes", wire.ChecksumCorrupt, plain, false, harness.DumboDBFull},
		{"snappy_valid_accepted", wire.ChecksumValid, snappy, false, harness.DumboDBFull},
		{"snappy_corrupt_closes", wire.ChecksumCorrupt, snappy, false, harness.DumboDBFull},
		{"valid_reply_checksummed", wire.ChecksumValid, plain, true, harness.DumboDBXFail},
		{"snappy_valid_reply_checksummed", wire.ChecksumValid, snappy, true, harness.DumboDBXFail},
		// mongod reads the document's last 4 bytes as the checksum and fails
		// parsing with InvalidBSON before checking it; DumboDB closes the connection.
		{"flag_without_checksum", wire.ChecksumFlagOnly, plain, true, harness.DumboDBXFail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWireParity(t, "checksum_"+tc.name, tc.support, harness.TopologyStandalone,
				func(addr, _ string) (interface{}, error) {
					c, err := wire.Dial(addr)
					if err != nil {
						return nil, fmt.Errorf("dial: %w", err)
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(15 * time.Second))

					ping := bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}}
					reply, err := c.RunChecksummedCommand(ping, tc.checksum, tc.compressor)
					if err != nil {
						return bson.D{{Key: "closed", Value: connectionClosed(err)}}, nil
					}
					result := bson.D{
						{Key: "ok", Value: reply["ok"]},
						{Key: "replyCompressor", Value: c.LastReplyCompressor()},
					}
					if tc.replyDetail {
						result = append(result,
							bson.E{Key: "code", Value: reply["code"]},
							bson.E{Key: "replyChecksum", Value: c.LastReplyChecksum()},
						)
					}
					return result, nil
				})
		})
	}
}
