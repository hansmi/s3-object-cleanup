package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestLogBuildInfo(t *testing.T) {
	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logBuildInfo(logger)

	var got map[string]any

	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	for _, key := range []string{
		"go_version",
		"main.path",
		"settings",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("Missing key %q: %q", key, got)
		}
	}
}
