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

// Package wire is a minimal MongoDB wire-protocol client for
// parity tests that need server behavior the Go driver hides. The MongoDB
// Driver Specification forbids drivers from accepting a caller-supplied lsid,
// so tests that share an lsid across connections must speak OP_MSG directly.
package wire

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/golang/snappy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	opCompressed int32 = 2012
	opMsg        int32 = 2013
)

// OP_COMPRESSED compressor ids.
const (
	CompressorNoop   byte = 0
	CompressorSnappy byte = 1
	CompressorZlib   byte = 2
)

// Conn is not safe for concurrent use.
type Conn struct {
	c     net.Conn
	reqID int32

	lastReplyCompressor int
	lastReplyChecksum   string
	traffic             Traffic
}

// Traffic totals what a Conn sent and received, in the terms mongod's
// serverStatus.network uses: logical sizes are whole uncompressed messages,
// wire sizes are what crossed the socket, and compressed payload and body sizes
// exclude the header and the OP_COMPRESSED prefix.
type Traffic struct {
	Requests            int64
	RequestLogicalBytes int64
	RequestWireBytes    int64
	RequestPayloadBytes int64
	RequestBodyBytes    int64
	ReplyLogicalBytes   int64
	ReplyWireBytes      int64
	ReplyPayloadBytes   int64
	ReplyBodyBytes      int64
}

// Traffic returns the totals since the connection was dialed.
func (c *Conn) Traffic() Traffic { return c.traffic }

// Dial accepts "host:port" or "mongodb://host:port".
func Dial(addr string) (*Conn, error) {
	hp, err := hostPort(addr)
	if err != nil {
		return nil, err
	}
	c, err := net.Dial("tcp", hp)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", hp, err)
	}
	return &Conn{c: c, lastReplyCompressor: -1, lastReplyChecksum: "none"}, nil
}

func (c *Conn) Close() error { return c.c.Close() }

func (c *Conn) SetDeadline(t time.Time) error { return c.c.SetDeadline(t) }

const (
	FlagMoreToCome     uint32 = 1 << 1
	FlagExhaustAllowed uint32 = 1 << 16
)

// RunCommand sends cmd as the body section of an OP_MSG.
// cmd must include "$db".
func (c *Conn) RunCommand(cmd interface{}) (bson.M, error) {
	reply, _, err := c.RunCommandFlags(cmd, 0)
	return reply, err
}

func (c *Conn) RunCommandFlags(cmd interface{}, flags uint32) (bson.M, uint32, error) {
	body, err := bson.Marshal(cmd)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal cmd: %w", err)
	}

	c.reqID++
	const headerLen = 16
	const flagBitsLen = 4
	const sectionKindLen = 1
	msgLen := int32(headerLen + flagBitsLen + sectionKindLen + len(body))

	out := make([]byte, 0, msgLen)
	out = appendI32(out, msgLen)
	out = appendI32(out, c.reqID)
	out = appendI32(out, 0) // responseTo
	out = appendI32(out, opMsg)
	out = appendI32(out, int32(flags))
	out = append(out, 0) // section kind 0 (body)
	out = append(out, body...)

	if _, err := c.c.Write(out); err != nil {
		return nil, 0, fmt.Errorf("write: %w", err)
	}
	c.traffic.Requests++
	c.traffic.RequestLogicalBytes += int64(len(out))
	c.traffic.RequestWireBytes += int64(len(out))
	return c.readCommandReply()
}

func (c *Conn) RunZlibCompressedCommand(cmd interface{}) (bson.M, error) {
	return c.RunCompressedCommand(cmd, CompressorZlib)
}

// RunCompressedCommand sends cmd as an OP_MSG wrapped in OP_COMPRESSED with the
// given compressor, whether or not the server advertised it.
func (c *Conn) RunCompressedCommand(cmd interface{}, compressor byte) (bson.M, error) {
	document, err := bson.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	uncompressed := make([]byte, 0, 5+len(document))
	uncompressed = appendI32(uncompressed, 0)
	uncompressed = append(uncompressed, 0)
	uncompressed = append(uncompressed, document...)

	compressed, err := compress(uncompressed, compressor)
	if err != nil {
		return nil, err
	}

	c.reqID++
	const headerLen = 16
	const compressedHeaderLen = 9
	msgLen := int32(headerLen + compressedHeaderLen + len(compressed))
	out := make([]byte, 0, msgLen)
	out = appendI32(out, msgLen)
	out = appendI32(out, c.reqID)
	out = appendI32(out, 0)
	out = appendI32(out, opCompressed)
	out = appendI32(out, opMsg)
	out = appendI32(out, int32(len(uncompressed)))
	out = append(out, compressor)
	out = append(out, compressed...)
	if _, err := c.c.Write(out); err != nil {
		return nil, fmt.Errorf("write compressed command: %w", err)
	}
	c.traffic.Requests++
	c.traffic.RequestLogicalBytes += int64(headerLen + len(uncompressed))
	c.traffic.RequestWireBytes += int64(len(out))
	c.traffic.RequestPayloadBytes += int64(len(compressed))
	c.traffic.RequestBodyBytes += int64(len(uncompressed))
	reply, _, err := c.readCommandReply()
	return reply, err
}

// LastReplyCompressor returns the compressor id of the last reply read, or -1
// when it arrived uncompressed.
func (c *Conn) LastReplyCompressor() int { return c.lastReplyCompressor }

// LastReplyChecksum reports the last reply's OP_MSG checksum: "none", "valid"
// or "invalid".
func (c *Conn) LastReplyChecksum() string { return c.lastReplyChecksum }

// Checksum selects what RunChecksummedCommand puts after an OP_MSG whose
// checksumPresent flag is set.
type Checksum int

const (
	ChecksumValid    Checksum = iota // correct CRC-32C
	ChecksumCorrupt                  // CRC-32C with bits flipped
	ChecksumFlagOnly                 // flag set, no checksum bytes
)

const checksumPresent uint32 = 1

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// RunChecksummedCommand sends cmd as an OP_MSG with checksumPresent set and the
// given checksum, wrapped in OP_COMPRESSED when compressor is not -1. The
// checksum covers the uncompressed message, header included.
func (c *Conn) RunChecksummedCommand(cmd interface{}, checksum Checksum, compressor int) (bson.M, error) {
	document, err := bson.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal cmd: %w", err)
	}
	const headerLen = 16
	msgLen := headerLen + 5 + len(document)
	if checksum != ChecksumFlagOnly {
		msgLen += crc32.Size
	}
	c.reqID++
	msg := make([]byte, 0, msgLen)
	msg = appendI32(msg, int32(msgLen))
	msg = appendI32(msg, c.reqID)
	msg = appendI32(msg, 0)
	msg = appendI32(msg, opMsg)
	msg = appendI32(msg, int32(checksumPresent))
	msg = append(msg, 0)
	msg = append(msg, document...)
	if checksum != ChecksumFlagOnly {
		sum := crc32.Checksum(msg, castagnoli)
		if checksum == ChecksumCorrupt {
			sum ^= 0xdeadbeef
		}
		msg = binary.LittleEndian.AppendUint32(msg, sum)
	}

	out := msg
	if compressor != -1 {
		compressed, err := compress(msg[headerLen:], byte(compressor))
		if err != nil {
			return nil, err
		}
		out = make([]byte, 0, headerLen+9+len(compressed))
		out = appendI32(out, int32(headerLen+9+len(compressed)))
		out = appendI32(out, c.reqID)
		out = appendI32(out, 0)
		out = appendI32(out, opCompressed)
		out = appendI32(out, opMsg)
		out = appendI32(out, int32(len(msg)-headerLen))
		out = append(out, byte(compressor))
		out = append(out, compressed...)
	}
	if _, err := c.c.Write(out); err != nil {
		return nil, fmt.Errorf("write checksummed command: %w", err)
	}
	c.traffic.Requests++
	c.traffic.RequestLogicalBytes += int64(len(msg))
	c.traffic.RequestWireBytes += int64(len(out))
	reply, _, err := c.readCommandReply()
	return reply, err
}

// RunRawOpMsg sends payload, the flag bits and sections of an OP_MSG, under a
// valid header, so tests can send messages a driver would refuse to build.
func (c *Conn) RunRawOpMsg(payload []byte) (bson.M, error) {
	c.reqID++
	const headerLen = 16
	out := make([]byte, 0, headerLen+len(payload))
	out = appendI32(out, int32(headerLen+len(payload)))
	out = appendI32(out, c.reqID)
	out = appendI32(out, 0) // responseTo
	out = appendI32(out, opMsg)
	out = append(out, payload...)

	if _, err := c.c.Write(out); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	c.traffic.Requests++
	c.traffic.RequestLogicalBytes += int64(len(out))
	c.traffic.RequestWireBytes += int64(len(out))
	reply, _, err := c.readCommandReply()
	return reply, err
}

func compress(body []byte, compressor byte) ([]byte, error) {
	switch compressor {
	case CompressorNoop:
		return body, nil
	case CompressorSnappy:
		return snappy.Encode(nil, body), nil
	case CompressorZlib:
		var compressed bytes.Buffer
		writer := zlib.NewWriter(&compressed)
		if _, err := writer.Write(body); err != nil {
			return nil, fmt.Errorf("compress cmd: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("finish compressed cmd: %w", err)
		}
		return compressed.Bytes(), nil
	}
	return nil, fmt.Errorf("unsupported compressor id %d", compressor)
}

func decompress(payload []byte, compressor byte) ([]byte, error) {
	switch compressor {
	case CompressorNoop:
		return payload, nil
	case CompressorSnappy:
		return snappy.Decode(nil, payload)
	case CompressorZlib:
		reader, err := zlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	}
	return nil, fmt.Errorf("unsupported reply compressor id %d", compressor)
}

// ReadReply reads the next OP_MSG reply, such as a further exhaust response.
func (c *Conn) ReadReply() (bson.M, uint32, error) {
	return c.readCommandReply()
}

func (c *Conn) readCommandReply() (bson.M, uint32, error) {
	const headerLen = 16
	const flagBitsLen = 4
	const sectionKindLen = 1
	var hdr [headerLen]byte
	if _, err := io.ReadFull(c.c, hdr[:]); err != nil {
		return nil, 0, fmt.Errorf("read header: %w", err)
	}
	replyLen := int32(binary.LittleEndian.Uint32(hdr[0:4]))
	if replyLen < headerLen+flagBitsLen+sectionKindLen {
		return nil, 0, fmt.Errorf("short OP_MSG reply: %d bytes", replyLen)
	}
	replyOp := int32(binary.LittleEndian.Uint32(hdr[12:16]))
	if replyOp != opMsg && replyOp != opCompressed {
		return nil, 0, fmt.Errorf("unexpected reply opcode %d (want OP_MSG)", replyOp)
	}

	rest := make([]byte, replyLen-headerLen)
	if _, err := io.ReadFull(c.c, rest); err != nil {
		return nil, 0, fmt.Errorf("read body: %w", err)
	}

	c.lastReplyCompressor = -1
	c.traffic.ReplyWireBytes += int64(replyLen)
	if replyOp == opCompressed {
		if len(rest) < 9 || int32(binary.LittleEndian.Uint32(rest[0:4])) != opMsg {
			return nil, 0, errors.New("malformed OP_COMPRESSED reply")
		}
		compressor := rest[8]
		body, err := decompress(rest[9:], compressor)
		if err != nil {
			return nil, 0, fmt.Errorf("decompress reply: %w", err)
		}
		c.traffic.ReplyPayloadBytes += int64(len(rest) - 9)
		c.traffic.ReplyBodyBytes += int64(len(body))
		rest = body
		c.lastReplyCompressor = int(compressor)
	}
	c.traffic.ReplyLogicalBytes += int64(headerLen + len(rest))

	// rest layout: [4 flagBits][1 sectionKind][body...]
	if len(rest) < 5 {
		return nil, 0, errors.New("malformed OP_MSG reply: truncated section header")
	}
	replyFlags := binary.LittleEndian.Uint32(rest[0:4])
	c.lastReplyChecksum = "none"
	if replyFlags&checksumPresent != 0 {
		if len(rest) < 5+crc32.Size {
			return nil, 0, errors.New("malformed OP_MSG reply: checksum flag without checksum")
		}
		// A compressed reply's checksum covers the message before compression,
		// so rebuild that header.
		var original [headerLen]byte
		copy(original[:], hdr[:])
		binary.LittleEndian.PutUint32(original[0:4], uint32(headerLen+len(rest)))
		binary.LittleEndian.PutUint32(original[12:16], uint32(opMsg))
		hasher := crc32.New(castagnoli)
		_, _ = hasher.Write(original[:])
		_, _ = hasher.Write(rest[:len(rest)-crc32.Size])
		c.lastReplyChecksum = "invalid"
		if hasher.Sum32() == binary.LittleEndian.Uint32(rest[len(rest)-crc32.Size:]) {
			c.lastReplyChecksum = "valid"
		}
		rest = rest[:len(rest)-crc32.Size]
	}
	sectionKind := rest[4]
	if sectionKind != 0 {
		return nil, 0, fmt.Errorf("unexpected section kind %d (only kind 0 supported)", sectionKind)
	}

	var reply bson.M
	if err := bson.Unmarshal(rest[5:], &reply); err != nil {
		return nil, 0, fmt.Errorf("decode reply: %w", err)
	}
	return reply, replyFlags, nil
}

// NewLsid returns a random UUID v4 as a BSON subtype-4 binary, suitable as
// the inner field of `lsid: { id: ... }`.
func NewLsid() primitive.Binary {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0F) | 0x40 // version 4
	b[8] = (b[8] & 0x3F) | 0x80 // RFC 4122 variant
	return primitive.Binary{Subtype: 0x04, Data: append([]byte(nil), b[:]...)}
}

func appendI32(buf []byte, v int32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	return append(buf, b[:]...)
}

func hostPort(addr string) (string, error) {
	if !strings.Contains(addr, "://") {
		return addr, nil
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", fmt.Errorf("parse uri: %w", err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("no host in URI %q", addr)
	}
	return u.Host, nil
}
