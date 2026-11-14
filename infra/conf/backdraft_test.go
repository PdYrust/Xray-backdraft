package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

func TestBackdraftConfigBuild(t *testing.T) {
	rawConfig := `{
		"target": "500GB"
	}`
	var config conf.BackdraftConfig
	if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
		t.Fatal(err)
	}
	b, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	if b.Target != "500GB" {
		t.Fatalf("unexpected target %q", b.Target)
	}

	rawConfig = `{
		"target": "2TB",
		"destination": "origin.example.com:80",
		"path": "/files/100Mb.dat",
		"objectSize": "100MB",
		"closeWindow": "4ms",
		"abort": "rst",
		"readBytes": 0,
		"rate": 20,
		"jitter": 30,
		"concurrency": 2,
		"warmup": "disable",
		"outbound": "proxy"
	}`
	if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
		t.Fatal(err)
	}
	b, err = config.Build()
	if err != nil {
		t.Fatal(err)
	}
	if b.Destination != "origin.example.com:80" || b.CloseWindow != "4ms" || b.Outbound != "proxy" {
		t.Fatalf("settings not carried over: %+v", b)
	}
}

func TestBackdraftConfigRequiresTarget(t *testing.T) {
	var config conf.BackdraftConfig
	if err := json.Unmarshal([]byte(`{}`), &config); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Build(); err == nil {
		t.Fatal("build without target must fail")
	}

	for _, rawConfig := range []string{
		`{"target": "500XB"}`,
		`{"target": "500GB", "abort": "boom"}`,
		`{"target": "500GB", "closeWindow": "soon"}`,
		`{"target": "500GB", "destination": "no-port"}`,
		`{"target": "500GB", "jitter": 101}`,
		`{"target": "500GB", "readBytes": -1}`,
	} {
		var c conf.BackdraftConfig
		if err := json.Unmarshal([]byte(rawConfig), &c); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Build(); err == nil {
			t.Fatalf("build must fail for %s", rawConfig)
		}
	}
}
