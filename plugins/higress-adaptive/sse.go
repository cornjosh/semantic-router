package main

import (
	"bytes"
	"encoding/json"
)

// An observer only: never writes response buffers. Oversize/malformed events
// make the first-token time unknown, rather than shifting it to a later token.
type sseParser struct {
	line, event                []byte
	limit                      int
	found, disabled, pendingCR bool
	firstLine                  bool
}

func newSSEParser(limit int) *sseParser { return &sseParser{limit: limit, firstLine: true} }
func (s *sseParser) feed(chunk []byte) bool {
	if s.found || s.disabled {
		return false
	}
	for _, b := range chunk {
		if s.pendingCR {
			s.pendingCR = false
			if b == '\n' {
				continue
			}
		}
		if b == '\n' || b == '\r' {
			s.pendingCR = b == '\r'
			if s.firstLine {
				s.line = bytes.TrimPrefix(s.line, []byte{0xef, 0xbb, 0xbf})
				s.firstLine = false
			}
			if len(s.line) == 0 {
				if len(s.event) > 0 {
					s.found = s.hasToken()
					s.event = nil
				}
			} else if bytes.HasPrefix(s.line, []byte("data:")) {
				v := s.line[5:]
				if len(v) > 0 && v[0] == ' ' {
					v = v[1:]
				}
				s.event = append(s.event, v...)
				s.event = append(s.event, '\n')
			}
			s.line = nil
			if s.found || s.disabled {
				return s.found
			}
		} else {
			s.line = append(s.line, b)
		}
		if len(s.line)+len(s.event) > s.limit {
			s.disabled = true
			s.line = nil
			s.event = nil
			return false
		}
	}
	return false
}
func (s *sseParser) hasToken() bool {
	data := bytes.TrimSpace(s.event)
	if bytes.Equal(data, []byte("[DONE]")) {
		return false
	}
	var event struct {
		Choices []struct {
			Delta map[string]json.RawMessage `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &event) != nil {
		s.disabled = true
		return false
	}
	for _, c := range event.Choices {
		for _, key := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
			if nonemptyString(c.Delta[key]) {
				return true
			}
		}
		var f struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		}
		if json.Unmarshal(c.Delta["function_call"], &f) == nil && (f.Name != "" || f.Arguments != "") {
			return true
		}
		var calls []struct {
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		}
		if json.Unmarshal(c.Delta["tool_calls"], &calls) == nil {
			for _, call := range calls {
				if call.Function.Name != "" || call.Function.Arguments != "" {
					return true
				}
			}
		}
	}
	return false
}
func nonemptyString(raw json.RawMessage) bool {
	var v string
	return json.Unmarshal(raw, &v) == nil && v != ""
}
