package main

import (
	"strings"
	"testing"
)

func TestSSEFragmentedCommentsAndRoleAreNotTokens(t *testing.T) {
	s := newSSEParser(256)
	prefix := ": ping\r\n\r\ndata: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\r\n\r\n"
	for _, b := range []byte(prefix) {
		if s.feed([]byte{b}) {
			t.Fatal("keepalive/role counted as token")
		}
	}
	event := "data: {\"choices\":\n" + "data: [{\"delta\":{\"content\":\"你好\"}}]}\r\n\r\n"
	// A lone CR terminates a line too; the subsequent LF belongs to the same delimiter.
	for i, b := range []byte(event) {
		if s.feed([]byte{b}) && i != len([]byte(event))-2 {
			t.Fatal("partial event counted")
		}
	}
	if !s.found {
		t.Fatal("content not detected")
	}
}

func TestSSEToolReasoningRefusalAndBounds(t *testing.T) {
	for _, delta := range []string{`{"tool_calls":[{"function":{"name":"weather"}}]}`, `{"function_call":{"arguments":"{"}}`, `{"reasoning_content":"think"}`, `{"refusal":"cannot"}`} {
		s := newSSEParser(256)
		if !s.feed([]byte("data: {\"choices\":[{\"delta\":" + delta + "}]}\n\n")) {
			t.Errorf("missed %s", delta)
		}
	}
	for _, event := range []string{"data: [DONE]\n\n", "data: invalid\n\n", "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\"}]}}]}\n\n"} {
		if newSSEParser(256).feed([]byte(event)) {
			t.Errorf("false token: %s", event)
		}
	}
	s := newSSEParser(256)
	s.feed([]byte("data: " + strings.Repeat("x", 10000)))
	if !s.disabled || len(s.line)+len(s.event) > 256 {
		t.Fatal("unbounded stream observer")
	}
	if s.feed([]byte("\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")) {
		t.Fatal("fabricated first token after overflow")
	}
}

func TestSSEUTF8BOMMayBeFragmented(t *testing.T) {
	s := newSSEParser(256)
	for _, b := range []byte{0xef, 0xbb, 0xbf} {
		if s.feed([]byte{b}) {
			t.Fatal("BOM counted")
		}
	}
	if !s.feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")) {
		t.Fatal("first event lost behind BOM")
	}
}
