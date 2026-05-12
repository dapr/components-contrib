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

package git

import (
	"bufio"
	"bytes"
	"fmt"
	"path"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/kit/logger"
)

// extKey returns the lowercased extension of p including the leading ".".
func extKey(p string) string {
	return strings.ToLower(path.Ext(p))
}

// fileEntry is the input to mapping strategies. RelPath is POSIX-style,
// without a leading slash, relative to the configured Path.
type fileEntry struct {
	RelPath string
	Bytes   []byte
}

// mappingStrategy converts file entries into a snapshot of configuration
// items. The version string is applied to every Item.Version.
type mappingStrategy interface {
	Name() string
	Map(entries []fileEntry, version string, log logger.Logger) (map[string]*configuration.Item, error)
}

func selectMapper(mode string) (mappingStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", mappingModeFile:
		return &fileMapper{}, nil
	case mappingModeAgentYAML:
		return &agentYAMLMapper{}, nil
	case mappingModePrompty:
		return &promptyMapper{}, nil
	}
	return nil, fmt.Errorf("unsupported mappingMode %q (supported: file, agentYaml, prompty)", mode)
}

// fileMapper maps each file to one configuration item with key=relpath,
// value=string(content).
type fileMapper struct{}

func (fileMapper) Name() string { return mappingModeFile }

func (fileMapper) Map(entries []fileEntry, version string, _ logger.Logger) (map[string]*configuration.Item, error) {
	out := make(map[string]*configuration.Item, len(entries))
	for _, e := range entries {
		out[e.RelPath] = &configuration.Item{
			Value:    string(e.Bytes),
			Version:  version,
			Metadata: map[string]string{},
		}
	}
	return out, nil
}

// agentYAMLMapper parses each *.yaml/*.yml/*.json file as a flat top-level
// map and emits one key per top-level field, prefixed by the filename stem.
//
// Non-matching files in the configured scope are a hard error: if the
// operator selected mappingMode=agentYaml and the path contains a README,
// LICENSE, or any other non-YAML file, that's a configuration mistake the
// operator should be told about — not silently dropped.
type agentYAMLMapper struct{}

func (agentYAMLMapper) Name() string { return mappingModeAgentYAML }

func (agentYAMLMapper) Map(entries []fileEntry, version string, log logger.Logger) (map[string]*configuration.Item, error) {
	out := make(map[string]*configuration.Item)
	owners := make(map[string]string)
	for _, e := range entries {
		switch extKey(e.RelPath) {
		case ".yaml", ".yml", ".json":
		default:
			return nil, fmt.Errorf("mappingMode=agentYaml does not accept %q: only .yaml/.yml/.json files are permitted; either move the file out of the configured path or switch to mappingMode=file", e.RelPath)
		}
		stem := stemOf(e.RelPath)
		fields, err := parseFlatMap(e.Bytes)
		if err != nil {
			return nil, fmt.Errorf("mappingMode=agentYaml: parse %q: %w", e.RelPath, err)
		}
		for k, v := range fields {
			key := stem + "/" + k
			if owner, exists := owners[key]; exists && log != nil {
				log.Warnf("git mapping: key %q from %q overwritten by %q", key, owner, e.RelPath)
			}
			owners[key] = e.RelPath
			out[key] = &configuration.Item{
				Value:    v,
				Version:  version,
				Metadata: map[string]string{},
			}
		}
	}
	return out, nil
}

// promptyMapper splits the YAML frontmatter from the body and emits keys for
// each frontmatter field plus an `agent_system_prompt` carrying the body.
//
// Non-.prompty files in the configured scope are a hard error: mixed-content
// directories must use mappingMode=file. The Prompty spec is at
// https://github.com/microsoft/prompty.
type promptyMapper struct{}

func (promptyMapper) Name() string { return mappingModePrompty }

func (promptyMapper) Map(entries []fileEntry, version string, log logger.Logger) (map[string]*configuration.Item, error) {
	out := make(map[string]*configuration.Item)
	owners := make(map[string]string)
	for _, e := range entries {
		if extKey(e.RelPath) != ".prompty" {
			return nil, fmt.Errorf("mappingMode=prompty does not accept %q: only .prompty files are permitted; either move the file out of the configured path or switch to mappingMode=file", e.RelPath)
		}
		stem := stemOf(e.RelPath)
		frontmatter, body, err := splitPromptyFrontmatter(e.Bytes)
		if err != nil {
			return nil, fmt.Errorf("mappingMode=prompty: %q: %w", e.RelPath, err)
		}
		if len(frontmatter) > 0 {
			fields, err := parseFlatMap(frontmatter)
			if err != nil {
				return nil, fmt.Errorf("mappingMode=prompty: parse frontmatter of %q: %w", e.RelPath, err)
			}
			for k, v := range fields {
				key := stem + "/" + k
				if owner, exists := owners[key]; exists && log != nil {
					log.Warnf("git mapping: key %q from %q overwritten by %q", key, owner, e.RelPath)
				}
				owners[key] = e.RelPath
				out[key] = &configuration.Item{
					Value:    v,
					Version:  version,
					Metadata: map[string]string{},
				}
			}
		}
		body = bytes.TrimSpace(body)
		if len(body) > 0 {
			key := stem + "/agent_system_prompt"
			if owner, exists := owners[key]; exists && log != nil {
				log.Warnf("git mapping: key %q from %q overwritten by %q", key, owner, e.RelPath)
			}
			owners[key] = e.RelPath
			out[key] = &configuration.Item{
				Value:    string(body),
				Version:  version,
				Metadata: map[string]string{},
			}
		}
	}
	return out, nil
}

// stemOf returns a deterministic, collision-free key prefix derived from the
// relative path. Directory separators are replaced with `_` so two files with
// the same basename in different subdirectories produce different stems.
// The result is lowercased to keep keys stable across case-insensitive
// filesystems. Examples:
//
//	"weather.yaml"            → "weather"
//	"agents/weather.yaml"     → "agents_weather"
//	"team-a/weather.prompty"  → "team-a_weather"
func stemOf(p string) string {
	if i := strings.LastIndex(p, "."); i > 0 {
		p = p[:i]
	}
	return strings.ToLower(strings.ReplaceAll(p, "/", "_"))
}

// parseFlatMap parses YAML/JSON bytes as a top-level map[string]any and
// returns string-encoded values: scalars via fmt, non-scalars via canonical
// YAML serialisation (lossless round-trip for the consumer).
func parseFlatMap(b []byte) (map[string]string, error) {
	var raw map[string]any
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("yaml decode: %w", err)
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		s, err := encodeFieldValue(v)
		if err != nil {
			return nil, fmt.Errorf("encode field %q: %w", k, err)
		}
		out[k] = s
	}
	return out, nil
}

func encodeFieldValue(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", x), nil
	}
	// Maps, slices, or anything else — re-serialise as YAML so the consumer
	// can round-trip without losing structure.
	out, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n"), nil
}

// splitPromptyFrontmatter accepts a prompty file and returns (frontmatter,
// body). The grammar is:
//
//	---\n
//	<yaml>\n
//	---\n
//	<body...>
//
// Files without a leading `---` are treated as body-only. Files with only the
// opening `---` (and no closing) are treated as frontmatter-only — the
// remainder is yaml. Returns an error if the file is too large for the
// scanner buffer (4 MiB) so the caller can skip rather than silently produce
// a truncated result.
func splitPromptyFrontmatter(b []byte) (frontmatter, body []byte, err error) {
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	lines := make([][]byte, 0)
	for scanner.Scan() {
		// Copy the slice — bufio's buffer is re-used.
		raw := scanner.Bytes()
		clone := make([]byte, len(raw))
		copy(clone, raw)
		lines = append(lines, clone)
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, nil, fmt.Errorf("scan prompty file: %w", scanErr)
	}
	if len(lines) == 0 {
		return nil, nil, nil
	}
	if !isFrontmatterDelim(lines[0]) {
		return nil, b, nil
	}
	for i := 1; i < len(lines); i++ {
		if isFrontmatterDelim(lines[i]) {
			frontmatter = bytes.Join(lines[1:i], []byte("\n"))
			if i+1 < len(lines) {
				body = bytes.Join(lines[i+1:], []byte("\n"))
			}
			return frontmatter, body, nil
		}
	}
	// Closing delimiter not found — treat all content after the opening
	// delimiter as frontmatter.
	frontmatter = bytes.Join(lines[1:], []byte("\n"))
	return frontmatter, nil, nil
}

func isFrontmatterDelim(line []byte) bool {
	return bytes.Equal(bytes.TrimRight(line, " \t\r"), []byte("---"))
}
