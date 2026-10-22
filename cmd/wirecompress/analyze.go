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

package main

import (
	"bytes"
	"compress/zlib"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// OP_COMPRESSED replaces everything after the 16-byte header with a 9-byte
// prefix (original opcode, uncompressed size, compressor id) and the payload.
const compressedPrefixLen = 9

// uncompressedCommands mirrors dumbodb internal/replication/transport, which
// follows the MongoDB compression spec: these are never compressed.
var uncompressedCommands = map[string]bool{
	"hello": true, "isMaster": true, "ismaster": true, "saslStart": true,
	"saslContinue": true, "authenticate": true, "getnonce": true, "createUser": true,
	"updateUser": true, "copydbSaslStart": true, "copydbgetnonce": true, "copydb": true,
}

type codec struct {
	name       string
	compress   func([]byte) []byte
	decompress func([]byte) []byte
}

// codecs uses the same libraries and default levels as dumbodb's outbound
// transport. zstdPerMessage additionally builds a new encoder per message, as
// transport.go does, to show what that costs.
func codecs() []codec {
	var zlibBuf bytes.Buffer
	zlibWriter := zlib.NewWriter(&zlibBuf)
	zstdEncoder, _ := zstd.NewWriter(nil)
	zstdDecoder, _ := zstd.NewReader(nil)
	return []codec{
		{"snappy",
			func(b []byte) []byte { return snappy.Encode(nil, b) },
			func(b []byte) []byte { out, _ := snappy.Decode(nil, b); return out }},
		{"zlib",
			func(b []byte) []byte {
				zlibBuf.Reset()
				zlibWriter.Reset(&zlibBuf)
				_, _ = zlibWriter.Write(b)
				_ = zlibWriter.Close()
				return append([]byte(nil), zlibBuf.Bytes()...)
			},
			func(b []byte) []byte {
				reader, _ := zlib.NewReader(bytes.NewReader(b))
				out, _ := io.ReadAll(reader)
				return out
			}},
		{"zstd",
			func(b []byte) []byte { return zstdEncoder.EncodeAll(b, nil) },
			func(b []byte) []byte { out, _ := zstdDecoder.DecodeAll(b, nil); return out }},
		{"zstdPerMessage",
			func(b []byte) []byte {
				encoder, _ := zstd.NewWriter(nil)
				defer encoder.Close()
				return encoder.EncodeAll(b, nil)
			},
			func(b []byte) []byte {
				decoder, _ := zstd.NewReader(nil)
				defer decoder.Close()
				out, _ := decoder.DecodeAll(b, nil)
				return out
			}},
	}
}

type groupKey struct {
	workload  string
	direction direction
	command   string
}

type groupStats struct {
	messages  int
	rawWire   int64
	codecWire map[string]int64
}

type cpuStats struct {
	inputBytes     int64
	compressTime   time.Duration
	decompressTime time.Duration
}

// analyze compresses every compressible captured message with each codec and
// returns wire sizes per group plus CPU cost per codec. Timings are the best of
// rounds passes over the whole corpus.
func analyze(messages []message, rounds int) (map[groupKey]*groupStats, map[string]cpuStats, error) {
	all := codecs()
	groups := map[groupKey]*groupStats{}
	var corpus [][]byte
	for _, m := range messages {
		if uncompressedCommands[m.command] {
			continue
		}
		key := groupKey{m.workload, m.direction, m.command}
		g := groups[key]
		if g == nil {
			g = &groupStats{codecWire: map[string]int64{}}
			groups[key] = g
		}
		body := m.bytes[headerLen:]
		g.messages++
		g.rawWire += int64(len(m.bytes))
		for _, c := range all {
			compressed := c.compress(body)
			if !bytes.Equal(c.decompress(compressed), body) {
				return nil, nil, fmt.Errorf("%s round trip mismatch on %s %s", c.name, m.direction, m.command)
			}
			g.codecWire[c.name] += int64(headerLen + compressedPrefixLen + len(compressed))
		}
		corpus = append(corpus, body)
	}

	cpu := map[string]cpuStats{}
	var input int64
	for _, body := range corpus {
		input += int64(len(body))
	}
	for _, c := range all {
		compressed := make([][]byte, len(corpus))
		best := cpuStats{inputBytes: input, compressTime: time.Duration(1<<63 - 1), decompressTime: time.Duration(1<<63 - 1)}
		for round := 0; round < rounds; round++ {
			runtime.GC()
			start := time.Now()
			for i, body := range corpus {
				compressed[i] = c.compress(body)
			}
			best.compressTime = min(best.compressTime, time.Since(start))
			runtime.GC()
			start = time.Now()
			for _, payload := range compressed {
				_ = c.decompress(payload)
			}
			best.decompressTime = min(best.decompressTime, time.Since(start))
		}
		cpu[c.name] = best
	}
	return groups, cpu, nil
}

func codecNames() []string {
	var names []string
	for _, c := range codecs() {
		names = append(names, c.name)
	}
	return names
}

func writeCSV(path, target string, groups map[groupKey]*groupStats, cpu map[string]cpuStats) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	names := codecNames()
	header := []string{"target", "workload", "direction", "command", "messages", "raw_wire_bytes"}
	for _, n := range names {
		header = append(header, n+"_wire_bytes")
	}
	_ = w.Write(header)
	keys := make([]groupKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.workload != b.workload {
			return a.workload < b.workload
		}
		if a.direction != b.direction {
			return a.direction < b.direction
		}
		return a.command < b.command
	})
	for _, k := range keys {
		g := groups[k]
		row := []string{target, k.workload, string(k.direction), k.command, strconv.Itoa(g.messages), strconv.FormatInt(g.rawWire, 10)}
		for _, n := range names {
			row = append(row, strconv.FormatInt(g.codecWire[n], 10))
		}
		_ = w.Write(row)
	}
	for _, n := range names {
		c := cpu[n]
		_ = w.Write([]string{target, "_cpu", n, "",
			strconv.FormatInt(c.inputBytes, 10),
			strconv.FormatInt(c.compressTime.Nanoseconds(), 10),
			strconv.FormatInt(c.decompressTime.Nanoseconds(), 10)})
	}
	w.Flush()
	return w.Error()
}

func printSummary(target string, groups map[groupKey]*groupStats, cpu map[string]cpuStats) {
	type total struct {
		raw   int64
		codec map[string]int64
	}
	byWorkload := map[string]*total{}
	var order []string
	for k, g := range groups {
		label := k.workload + " " + string(k.direction)
		t := byWorkload[label]
		if t == nil {
			t = &total{codec: map[string]int64{}}
			byWorkload[label] = t
			order = append(order, label)
		}
		t.raw += g.rawWire
		for n, v := range g.codecWire {
			t.codec[n] += v
		}
	}
	sort.Strings(order)
	names := []string{"snappy", "zlib", "zstd"}
	fmt.Printf("\n%s: wire bytes, and size after compression as a percentage of raw\n", target)
	fmt.Printf("%-32s %12s", "workload direction", "raw")
	for _, n := range names {
		fmt.Printf(" %8s", n)
	}
	fmt.Println()
	for _, label := range order {
		t := byWorkload[label]
		fmt.Printf("%-32s %12d", label, t.raw)
		for _, n := range names {
			fmt.Printf(" %7.1f%%", 100*float64(t.codec[n])/float64(t.raw))
		}
		fmt.Println()
	}
	fmt.Printf("\n%s: codec throughput over the compressible corpus\n", target)
	for _, n := range codecNames() {
		c := cpu[n]
		mb := float64(c.inputBytes) / 1e6
		fmt.Printf("%-16s compress %8.1f MB/s   decompress %8.1f MB/s\n", n, mb/c.compressTime.Seconds(), mb/c.decompressTime.Seconds())
	}
}
