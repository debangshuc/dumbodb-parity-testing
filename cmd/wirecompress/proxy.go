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
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

const (
	headerLen     = 16
	opReply       = 1
	opQuery       = 2004
	opMsg         = 2013
	checksumFlag  = 1
	maxMessageLen = 48 * 1000 * 1000
)

type direction string

const (
	toServer   direction = "request"
	fromServer direction = "reply"
)

// message is one captured wire message, header included.
type message struct {
	workload  string
	direction direction
	command   string
	bytes     []byte
}

type recorder struct {
	target   string
	workload atomic.Pointer[string]

	mu       sync.Mutex
	messages []message
}

func newRecorder(target string) *recorder {
	r := &recorder{target: target}
	r.setWorkload("setup")
	return r
}

func (r *recorder) setWorkload(name string) { r.workload.Store(&name) }

func (r *recorder) add(m message) {
	r.mu.Lock()
	r.messages = append(r.messages, m)
	r.mu.Unlock()
}

// serve accepts client connections on ln and relays each to the target,
// recording every message in both directions.
func (r *recorder) serve(ln net.Listener) {
	for {
		client, err := ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("accept: %v", err)
			}
			return
		}
		go r.relay(client)
	}
}

func (r *recorder) relay(client net.Conn) {
	server, err := net.Dial("tcp", r.target)
	if err != nil {
		log.Printf("dial %s: %v", r.target, err)
		_ = client.Close()
		return
	}
	var pendingMu sync.Mutex
	pending := map[int32]string{}

	closeBoth := func() {
		_ = client.Close()
		_ = server.Close()
	}
	go func() {
		defer closeBoth()
		r.pump(client, server, toServer, func(requestID, _ int32, command string) string {
			pendingMu.Lock()
			pending[requestID] = command
			pendingMu.Unlock()
			return command
		})
	}()
	go func() {
		defer closeBoth()
		r.pump(server, client, fromServer, func(_, responseTo int32, _ string) string {
			pendingMu.Lock()
			defer pendingMu.Unlock()
			command := pending[responseTo]
			delete(pending, responseTo)
			return command
		})
	}()
}

// pump copies whole messages from src to dst. name resolves the command a
// message belongs to: requests name themselves, replies inherit their request's.
func (r *recorder) pump(src io.Reader, dst io.Writer, dir direction, name func(requestID, responseTo int32, command string) string) {
	header := make([]byte, headerLen)
	for {
		if _, err := io.ReadFull(src, header); err != nil {
			return
		}
		length := int32(binary.LittleEndian.Uint32(header[0:4]))
		if length < headerLen || length > maxMessageLen {
			log.Printf("%s: bad message length %d", dir, length)
			return
		}
		msg := make([]byte, length)
		copy(msg, header)
		if _, err := io.ReadFull(src, msg[headerLen:]); err != nil {
			return
		}
		if _, err := dst.Write(msg); err != nil {
			return
		}
		requestID := int32(binary.LittleEndian.Uint32(msg[4:8]))
		responseTo := int32(binary.LittleEndian.Uint32(msg[8:12]))
		opcode := int32(binary.LittleEndian.Uint32(msg[12:16]))
		command := name(requestID, responseTo, commandName(opcode, msg[headerLen:]))
		r.add(message{workload: *r.workload.Load(), direction: dir, command: command, bytes: msg})
	}
}

// commandName returns the first key of the command document, or "" for a
// message that carries none (replies, OP_REPLY).
func commandName(opcode int32, body []byte) string {
	switch opcode {
	case opMsg:
		if len(body) < 4 {
			return ""
		}
		flags := binary.LittleEndian.Uint32(body[0:4])
		sections := body[4:]
		if flags&checksumFlag != 0 && len(sections) >= 4 {
			sections = sections[:len(sections)-4]
		}
		for len(sections) > 0 {
			kind := sections[0]
			sections = sections[1:]
			if len(sections) < 4 {
				return ""
			}
			size := int(binary.LittleEndian.Uint32(sections[0:4]))
			if size > len(sections) {
				return ""
			}
			if kind == 0 {
				return firstKey(sections[:size])
			}
			sections = sections[size:]
		}
	case opQuery:
		if len(body) < 4 {
			return ""
		}
		rest := body[4:]
		end := bytes.IndexByte(rest, 0)
		if end < 0 || len(rest) < end+1+8 {
			return ""
		}
		return firstKey(rest[end+1+8:])
	}
	return ""
}

func firstKey(doc []byte) string {
	if len(doc) < 6 {
		return ""
	}
	elements := doc[5:]
	end := bytes.IndexByte(elements, 0)
	if end < 0 {
		return ""
	}
	return string(elements[:end])
}
