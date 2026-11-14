package backdraft

import (
	"testing"
	"time"
)

func TestParseVolume(t *testing.T) {
	cases := []struct {
		input    string
		expected int64
	}{
		{"1024", 1024},
		{"512B", 512},
		{"1KB", 1 << 10},
		{"1KiB", 1 << 10},
		{"500MB", 500 << 20},
		{"500GB", 500 << 30},
		{"2TB", 2 << 40},
		{"1PB", 1 << 50},
		{" 10 GB ", 10 << 30},
		{"10gb", 10 << 30},
	}
	for _, c := range cases {
		got, err := ParseVolume(c.input)
		if err != nil {
			t.Fatalf("ParseVolume(%q) failed: %v", c.input, err)
		}
		if got != c.expected {
			t.Fatalf("ParseVolume(%q) = %d, want %d", c.input, got, c.expected)
		}
	}

	for _, input := range []string{"", "GB", "-5GB", "5XB", "5 G B", "1.5GB"} {
		if _, err := ParseVolume(input); err == nil {
			t.Fatalf("ParseVolume(%q) should fail", input)
		}
	}
}

func TestFormatVolume(t *testing.T) {
	cases := []struct {
		input    int64
		expected string
	}{
		{130, "130B"},
		{183 << 10, "183.0KB"},
		{500 << 20, "500.0MB"},
		{500 << 30, "500.0GB"},
	}
	for _, c := range cases {
		if got := formatVolume(c.input); got != c.expected {
			t.Fatalf("formatVolume(%d) = %q, want %q", c.input, got, c.expected)
		}
	}
}

func TestEngineDefaults(t *testing.T) {
	e, err := newEngine(&Config{Target: "500GB"})
	if err != nil {
		t.Fatal(err)
	}
	if e.abort != abortRST || !e.warmup {
		t.Fatalf("unexpected defaults: %+v", e)
	}
	if !e.adaptive || e.concurrency != initialConcurrency || e.limit != initialConcurrency || e.maxWorker != maxAutoConcurrency {
		t.Fatalf("unexpected pool defaults: %+v", e)
	}
	if len(e.destinations) == 0 {
		t.Fatal("expected auto destinations")
	}
	if e.window != time.Millisecond {
		t.Fatalf("unexpected initial window %v", e.window)
	}

	if _, err := newEngine(&Config{}); err == nil {
		t.Fatal("target must be required")
	}
	if _, err := newEngine(&Config{Target: "500GB", Abort: "boom"}); err == nil {
		t.Fatal("unknown abort must fail")
	}
	if _, err := newEngine(&Config{Target: "500GB", CloseWindow: "soon"}); err == nil {
		t.Fatal("invalid closeWindow must fail")
	}
	if _, err := newEngine(&Config{Target: "500GB", Destination: "no-port"}); err == nil {
		t.Fatal("destination without port must fail")
	}

	e, err = newEngine(&Config{Target: "1TB", Destination: "origin.example.com:8080", Path: "/blob", CloseWindow: "3ms", Abort: "fin", Warmup: "disable", Concurrency: 4, Rate: 10, Jitter: 50, ObjectSize: "100MB"})
	if err != nil {
		t.Fatal(err)
	}
	if e.window != 3*time.Millisecond || e.abort != abortFIN || e.warmup || e.concurrency != 4 {
		t.Fatalf("explicit settings not applied: %+v", e)
	}
	if e.destinations[0].host != "origin.example.com" || e.destinations[0].port != 8080 {
		t.Fatalf("destination not parsed: %+v", e.destinations[0])
	}
	if e.objectSize != 100<<20 {
		t.Fatalf("objectSize not parsed: %d", e.objectSize)
	}
}

func TestObserveAdaptation(t *testing.T) {
	e, err := newEngine(&Config{Target: "500GB", CloseWindow: "2ms"})
	if err != nil {
		t.Fatal(err)
	}

	// A clean cycle keeps the configured window untouched.
	e.observe(cycleResult{window: 2 * time.Millisecond, leak: 6})
	if e.window != 2*time.Millisecond {
		t.Fatalf("fixed window must not move: %v", e.window)
	}

	auto, err := newEngine(&Config{Target: "500GB"})
	if err != nil {
		t.Fatal(err)
	}
	start := auto.window
	auto.observe(cycleResult{window: start, leak: 6})
	if auto.window <= start {
		t.Fatalf("window should grow after a clean cycle: %v -> %v", start, auto.window)
	}

	// A leak moves the cliff estimate down conservatively and pulls the
	// window hard under it: detection lags the wire.
	leak := cycleResult{window: auto.window, leak: 512 << 10}
	auto.observe(leak)
	if auto.cliff != leak.window/2 {
		t.Fatalf("cliff not recorded: %v", auto.cliff)
	}
	if auto.window != leak.window/4 {
		t.Fatalf("window not backed off: %v", auto.window)
	}
	if auto.rate >= autoRateStart {
		t.Fatalf("rate not backed off: %v", auto.rate)
	}

	// Growth stops well below the recorded cliff.
	for i := 0; i < 20; i++ {
		auto.observe(cycleResult{window: auto.window, leak: 6})
	}
	if cap := auto.cliff * 3 / 5; auto.window > cap {
		t.Fatalf("window %v exceeds cliff cap %v", auto.window, cap)
	}
}

func TestEstimateBurn(t *testing.T) {
	e, err := newEngine(&Config{Target: "500GB", ObjectSize: "50MB"})
	if err != nil {
		t.Fatal(err)
	}
	e.recordTTFB(time.Millisecond)
	e.lastProbe = time.Now()

	burn := e.estimateBurn(cycleResult{window: 5 * time.Millisecond})
	if burn <= 0 {
		t.Fatalf("expected positive burn, got %d", burn)
	}
	if capped := e.estimateBurn(cycleResult{window: time.Hour, firstByte: time.Millisecond, leak: 1}); capped != cycleBurnCap {
		t.Fatalf("burn must be capped by the pipeline reservoir, got %d", capped)
	}
	if floor := e.estimateBurn(cycleResult{window: time.Microsecond, leak: 7 << 20}); floor != 7<<20 {
		t.Fatalf("burn must cover observed leak, got %d", floor)
	}
	if evidenced := e.estimateBurn(cycleResult{window: time.Second, firstByte: 300 * time.Millisecond, leak: 4096}); evidenced <= 4<<10 {
		t.Fatalf("live-probe cycle must burn above connection cost, got %d", evidenced)
	}

	// Without recent probes the burn stays at connection cost.
	e.lastProbe = time.Time{}
	dead := e.estimateBurn(cycleResult{window: time.Second, firstByte: 300 * time.Millisecond, leak: 4096})
	if dead != 4096 {
		t.Fatalf("stale-probe cycle must be credited at leak only, got %d", dead)
	}
}

func TestRequestSize(t *testing.T) {
	e, err := newEngine(&Config{Target: "500GB"})
	if err != nil {
		t.Fatal(err)
	}
	req := e.requestBytes(origin{host: "example.com", path: "/files/100Mb.dat"})
	if len(req) > 256 {
		t.Fatalf("request must stay small to keep the billed footprint tiny, got %d bytes", len(req))
	}
	if e.requestBytes(origin{host: "example.com"})[4] != '/' {
		t.Fatal("default path must be /")
	}
}
