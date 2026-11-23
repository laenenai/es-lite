package main

import (
	"flag"
	"testing"
)

func TestTagScenarioParses(t *testing.T) {
	fs := flag.NewFlagSet("tag-scenario", flag.ContinueOnError)
	tags := addTagFlags(fs)
	if err := fs.Parse([]string{"-tags", "-tag-space", "s1", "-tag-block-prefix", "blk"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !tags.enabled || tags.space != "s1" || tags.blockPrefix != "blk" {
		t.Fatalf("tags = %+v, want enabled with space s1 and block prefix blk", tags)
	}

	plain := flag.NewFlagSet("plain", flag.ContinueOnError)
	defaults := addTagFlags(plain)
	if err := plain.Parse(nil); err != nil {
		t.Fatalf("parse defaults: %v", err)
	}
	if defaults.enabled {
		t.Fatalf("tags enabled by default: %+v", defaults)
	}
	if defaults.space == "" || defaults.blockPrefix == "" {
		t.Fatalf("empty tag default: %+v", defaults)
	}
}
