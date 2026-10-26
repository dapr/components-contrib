/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/contenttype"
	daprmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
)

const (
	etagMismatchReply = "ETAGMISMATCH"
	multiArgsPerOp    = 5

	// Checks every guard before the first write. ARGV per key: kind, etag, json, data, ttl.
	//nolint:dupword
	multiQuery = `
local pend = {}
local function num(s)
  if not string.match(s, "^%-?%d+$") or #s > 15 then return nil end
  return tonumber(s)
end
local function keytype(key)
  local t = redis.call("TYPE", key)
  if type(t) == "table" then t = t["ok"] end
  return t
end
local function cur(key)
  local p = pend[key]
  if p ~= nil then return p end
  local t = keytype(key)
  if t == "none" then
    p = {exists=false, vs="", kind="none", fwr=false}
  elseif t == "hash" then
    local v = redis.call("HGET", key, "version")
    p = {exists=true, vs=v or "", kind="hash", fwr=(redis.call("HEXISTS", key, "first-write") == 1)}
  elseif t == "ReJSON-RL" then
    local v = redis.pcall("JSON.GET", key, ".version")
    if type(v) ~= "string" then v = "" end
    local f = redis.pcall("JSON.GET", key, ".first-write")
    p = {exists=true, vs=v, kind=t, fwr=(type(f) == "string")}
  else
    p = {exists=true, vs="", kind=t, fwr=false}
  end
  return p
end
local n = #KEYS
for i = 1, n do
  local key = KEYS[i]
  local b = (i - 1) * 5
  local kind, etag, js = ARGV[b+1], ARGV[b+2], ARGV[b+3]
  local p = cur(key)
  local target = "hash"
  if js == "1" then target = "ReJSON-RL" end
  if etag ~= "" then
    if not p.exists or num(p.vs) ~= tonumber(etag) then
      return redis.error_reply("` + etagMismatchReply + ` " .. key)
    end
  elseif kind == "s" and p.fwr and p.kind == target and p.vs ~= "" then
    return redis.error_reply("` + etagMismatchReply + ` " .. key)
  end
  if kind == "d" then
    pend[key] = {exists=false, vs="", kind="none", fwr=false}
  else
    local nv = 1
    if p.kind == target and p.vs ~= "" then
      local v = num(p.vs)
      if v == nil then
        return redis.error_reply("ERR non numeric version for key " .. key)
      end
      if v >= 99999999999999 then
        return redis.error_reply("ERR version too large for key " .. key)
      end
      nv = v + 1
    end
    pend[key] = {exists=true, vs=string.format("%d", nv), kind=target, fwr=(target == "hash" and p.kind == "hash" and p.fwr)}
  end
end
for i = 1, n do
  local key = KEYS[i]
  local b = (i - 1) * 5
  local kind, js, data, ttl = ARGV[b+1], ARGV[b+3], ARGV[b+4], ARGV[b+5]
  if kind == "d" then
    redis.call("DEL", key)
  else
    local t = keytype(key)
    if js == "1" then
      local prev = 0
      if t == "ReJSON-RL" then
        local v = redis.pcall("JSON.GET", key, ".version")
        if type(v) == "string" then prev = tonumber(v) or 0 end
      elseif t ~= "none" then
        redis.call("DEL", key)
      end
      redis.call("JSON.SET", key, "$", data)
      redis.call("JSON.SET", key, ".version", prev + 1)
    else
      if t ~= "hash" and t ~= "none" then
        redis.call("DEL", key)
      end
      redis.call("HSET", key, "data", data)
      redis.call("HINCRBY", key, "version", 1)
    end
    if ttl ~= "" then
      if tonumber(ttl) > 0 then
        redis.call("EXPIRE", key, ttl)
      else
        redis.call("PERSIST", key)
      end
    end
  end
end
return n`
)

var errJSONUnsupported = errors.New("redis transaction: content type application/json requires RedisJSON on the server")

// Far below Redis's EXPIRE overflow so the script never fails after it wrote.
const maxTTLSeconds int64 = math.MaxInt64 / 1000 / 2

type multiOp struct {
	key  string
	args [multiArgsPerOp]any
}

func (r *StateStore) isCluster() bool {
	return r.clientSettings != nil && r.clientSettings.RedisType == rediscomponent.ClusterType && !r.clientSettings.Failover
}

func (r *StateStore) buildMultiOps(request *state.TransactionalStateRequest) ([]multiOp, error) {
	isJSON := request.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && r.clientHasJSON

	ops := make([]multiOp, 0, len(request.Operations))
	for _, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			reqJSON := req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType
			if reqJSON && !r.clientHasJSON {
				return nil, errJSONUnsupported
			}
			op, err := r.setOp(&req, isJSON || reqJSON)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case state.DeleteRequest:
			if req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && !r.clientHasJSON {
				return nil, errJSONUnsupported
			}
			etag := ""
			if req.HasETag() {
				v, err := strconv.ParseInt(*req.ETag, 10, 64)
				if err != nil {
					return nil, state.NewETagError(state.ETagInvalid, err)
				}
				etag = strconv.FormatInt(v, 10)
			}
			ops = append(ops, multiOp{key: req.Key, args: [multiArgsPerOp]any{"d", etag, "0", "", ""}})
		}
	}
	return ops, nil
}

func (r *StateStore) setOp(req *state.SetRequest, isJSON bool) (multiOp, error) {
	ver, err := r.parseETag(req)
	if err != nil {
		return multiOp{}, err
	}
	etag := ""
	if req.HasETag() && req.Options.Concurrency != state.LastWrite {
		etag = strconv.Itoa(ver)
	}
	ttl, err := r.parseTTL(req)
	if err != nil {
		return multiOp{}, fmt.Errorf("failed to parse ttl from metadata: %w", err)
	}
	if ttl == nil {
		ttl = r.clientSettings.TTLInSeconds
	}
	ttlArg := ""
	if ttl != nil {
		if int64(*ttl) > maxTTLSeconds {
			return multiOp{}, fmt.Errorf("ttlInSeconds %d is larger than the maximum %d", *ttl, maxTTLSeconds)
		}
		ttlArg = strconv.Itoa(*ttl)
	}

	var bt []byte
	jsonArg := "0"
	if isJSON {
		jsonArg = "1"
		bt, _ = utils.Marshal(&jsonEntry{Data: req.Value}, r.json.Marshal)
	} else {
		bt, _ = utils.Marshal(req.Value, r.json.Marshal)
	}
	return multiOp{key: req.Key, args: [multiArgsPerOp]any{"s", etag, jsonArg, bt, ttlArg}}, nil
}

// Redis Cluster rejects a multi-key script across hash slots.
func (r *StateStore) execMulti(ctx context.Context, ops []multiOp) error {
	if len(ops) == 0 {
		return nil
	}

	batches := [][]multiOp{ops}
	if r.isCluster() {
		batches = groupBySlot(ops)
	}

	for _, batch := range batches {
		args := make([]any, 0, 3+len(batch)*(multiArgsPerOp+1))
		args = append(args, "EVAL", multiQuery, len(batch))
		for _, op := range batch {
			args = append(args, op.key)
		}
		for _, op := range batch {
			args = append(args, op.args[:]...)
		}
		if err := r.client.DoWrite(ctx, args...); err != nil {
			if strings.HasPrefix(err.Error(), etagMismatchReply) {
				return state.NewETagError(state.ETagMismatch, err)
			}
			return fmt.Errorf("redis transaction failed: %w", err)
		}
	}
	return nil
}

func groupBySlot(ops []multiOp) [][]multiOp {
	idx := make(map[uint16]int)
	var batches [][]multiOp
	for _, op := range ops {
		slot := keySlot(op.key)
		i, ok := idx[slot]
		if !ok {
			i = len(batches)
			idx[slot] = i
			batches = append(batches, nil)
		}
		batches[i] = append(batches[i], op)
	}
	return batches
}

func keySlot(key string) uint16 {
	if s := strings.IndexByte(key, '{'); s >= 0 {
		if e := strings.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+1+e]
		}
	}
	var crc uint16
	for i := range len(key) {
		crc ^= uint16(key[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc % 16384
}
