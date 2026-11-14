package backdraft

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

const (
	// The close window is hill-climbed between these bounds: grow while every
	// cycle stays invisible on the tunnel, back off hard as soon as one leaks.
	// On real paths the flush only starts once the origin has answered, so the
	// hunter needs headroom past the first-byte time to find the real cliff.
	minCloseWindow = 250 * time.Microsecond
	maxCloseWindow = 10 * time.Second

	// Downlink that stays within this budget is mux framing residue, not a leak.
	leakTolerance = 1024

	// Burn is credited at model value only while the tunnel probes keep
	// succeeding; once they stop, cycles are assumed idle instead of burning.
	probeTTL = 2 * time.Minute

	// A single cycle can only burn what the server-side pipeline can absorb
	// before backpressure reaches the origin: the per-connection pipe plus
	// kernel socket buffers. Measured burns sit in the 130KB-800KB range.
	cycleBurnCap = 1 << 20

	// Pool bounds for the automatic concurrency hunt. Throughput scales with
	// parallel cycles: each one holds its own server-side reservoir.
	initialConcurrency = 8
	maxAutoConcurrency = 128

	// Seed for the server-side ingest estimate (bytes per millisecond),
	// replaced by the throughput measured during probes as soon as one runs.
	ingestSeed = 32 * 1024

	autoRateMin   = 0.2
	autoRateMax   = 50.0
	autoRateStart = 10.0
	autoJitter    = 25
)

type origin struct {
	host string
	port uint16
	path string
	size int64
}

// autoOrigins are public bulk-transfer test endpoints, used as burn origins
// when no destination is configured. All serve large objects over plain HTTP
// without throttling repeated fetches, and rotate to spread the load.
var autoOrigins = []origin{
	{host: "cachefly.cachefly.net", port: 80, path: "/100mb.test", size: 100 << 20},
	{host: "ipv4.download.thinkbroadband.com", port: 80, path: "/1GB.zip", size: 1 << 30},
	{host: "ping.online.net", port: 80, path: "/100Mo.dat", size: 100 << 20},
}

type abortMode int

const (
	abortRST abortMode = iota
	abortFIN
)

type cycleResult struct {
	burn      int64
	billed    int64
	leak      int64
	firstByte time.Duration
	window    time.Duration
	elapsed   time.Duration
	missed    bool
	failed    bool
}

type engine struct {
	path       string
	objectSize int64
	abort      abortMode
	readBytes  int64
	warmup     bool
	outbound   string

	fixedRate    float64
	jitter       int
	concurrency  int
	adaptive     bool
	fixedWindow  time.Duration
	destinations []origin

	rng *rand.Rand

	mu        sync.Mutex
	window    time.Duration
	cliff     time.Duration
	ttfb      time.Duration
	ingest    float64
	measured  bool
	lastProbe time.Time
	rate      float64
	cursor    int
	limit     int
	maxWorker int
	sinceTune int
	tuneMiss  int
}

func newEngine(c *Config) (*engine, error) {
	e := &engine{
		path:      c.GetPath(),
		readBytes: c.GetReadBytes(),
		outbound:  c.GetOutbound(),
		ingest:    ingestSeed,
		rate:      autoRateStart,
		jitter:    autoJitter,
		window:    time.Millisecond,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	if _, err := ParseVolume(c.GetTarget()); err != nil {
		return nil, errors.New("invalid target").Base(err)
	}
	if c.GetObjectSize() != "" {
		size, err := ParseVolume(c.GetObjectSize())
		if err != nil {
			return nil, errors.New("invalid objectSize").Base(err)
		}
		e.objectSize = size
	}

	switch strings.ToLower(c.GetAbort()) {
	case "", "auto", "rst":
		e.abort = abortRST
	case "fin":
		e.abort = abortFIN
	default:
		return nil, errors.New("unknown abort mode: ", c.GetAbort())
	}

	switch strings.ToLower(c.GetWarmup()) {
	case "", "auto", "enable":
		e.warmup = true
	case "disable":
		e.warmup = false
	default:
		return nil, errors.New("unknown warmup mode: ", c.GetWarmup())
	}

	if e.readBytes < 0 {
		return nil, errors.New("readBytes must not be negative")
	}
	if c.GetRate() < 0 {
		return nil, errors.New("rate must not be negative")
	}
	e.fixedRate = float64(c.GetRate())
	if e.fixedRate > 0 {
		e.rate = e.fixedRate
	}

	if c.GetJitter() < 0 || c.GetJitter() > 100 {
		return nil, errors.New("jitter must be within 0-100")
	}
	if c.GetJitter() > 0 {
		e.jitter = int(c.GetJitter())
	}

	if c.GetConcurrency() < 0 {
		return nil, errors.New("concurrency must not be negative")
	}
	e.concurrency = int(c.GetConcurrency())
	if e.concurrency == 0 {
		e.concurrency = initialConcurrency
		e.maxWorker = maxAutoConcurrency
		e.adaptive = true
	} else {
		e.maxWorker = e.concurrency
	}
	e.limit = e.concurrency

	if c.GetCloseWindow() != "" && strings.ToLower(c.GetCloseWindow()) != "auto" {
		d, err := time.ParseDuration(c.GetCloseWindow())
		if err != nil {
			return nil, errors.New("invalid closeWindow").Base(err)
		}
		if d < 0 {
			return nil, errors.New("closeWindow must not be negative")
		}
		e.fixedWindow = d
		e.window = d
	}

	dest, err := parseDestinations(c.GetDestination(), e.path)
	if err != nil {
		return nil, err
	}
	if len(dest) > 0 {
		e.destinations = dest
	} else {
		e.destinations = autoOrigins
	}

	return e, nil
}

// parseDestinations accepts a comma-separated list of host:port burn origins.
func parseDestinations(address, path string) ([]origin, error) {
	if address == "" {
		return nil, nil
	}
	if path != "" && !strings.HasPrefix(path, "/") {
		return nil, errors.New("path must start with /")
	}
	var out []origin
	for _, part := range strings.Split(address, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(part))
		if err != nil {
			return nil, errors.New("destination must be host:port: ", part).Base(err)
		}
		p, err := strconv.ParseUint(port, 10, 16)
		if err != nil || p == 0 {
			return nil, errors.New("invalid destination port: ", port)
		}
		out = append(out, origin{host: host, port: uint16(p), path: path})
	}
	return out, nil
}

func (e *engine) nextOrigin() origin {
	e.mu.Lock()
	o := e.destinations[e.cursor%len(e.destinations)]
	e.cursor++
	e.mu.Unlock()
	if o.path == "" {
		o.path = "/"
	}
	return o
}

// currentLimit returns how many cycles may run in parallel right now.
func (e *engine) currentLimit() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.limit
}

// nextDelay returns the pause before the next cycle, applying jitter around
// the current rate so the cadence does not itself become a fingerprint.
func (e *engine) nextDelay() time.Duration {
	e.mu.Lock()
	rate := e.rate
	e.mu.Unlock()
	if rate <= 0 {
		rate = autoRateStart
	}
	delay := float64(time.Second) / rate
	if e.jitter > 0 {
		delta := (e.rng.Float64()*2 - 1) * float64(e.jitter) / 100
		delay *= 1 + delta
	}
	if delay < float64(time.Millisecond) {
		delay = float64(time.Millisecond)
	}
	return time.Duration(delay)
}

// recordTTFB folds a measured first-byte delay into the adaptation state and
// moves the close window near it: the real cliff sits just past the first byte.
// Delivery lag inflates individual samples, so the tracker latches onto the
// fastest observation and only creeps upward.
func (e *engine) recordTTFB(ttfb time.Duration) {
	if ttfb <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ttfb == 0 {
		e.ttfb = ttfb
		if e.fixedWindow == 0 && e.window < ttfb/2 {
			e.window = ttfb / 2
		}
	} else if ttfb < e.ttfb {
		e.ttfb = ttfb
	} else {
		e.ttfb = (e.ttfb*7 + ttfb) / 8
	}
}

// observe folds a finished cycle into the adaptation state. A clean cycle
// pushes the close window toward the cliff; a leaking one moves the cliff
// estimate down and halves the cadence until the window settles again.
func (e *engine) observe(r cycleResult) {
	if r.failed {
		return
	}

	if r.firstByte > 0 {
		e.recordTTFB(r.firstByte)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// The pool hunt: every parallel cycle holds its own server-side reservoir,
	// so grow while the path stays clean and back off when it leaks.
	if e.adaptive {
		e.sinceTune++
		if e.sinceTune >= 20 {
			e.sinceTune = 0
			if e.tuneMiss == 0 {
				// Double while small, then ease toward the ceiling.
				if e.limit < e.maxWorker/4 {
					e.limit = min(e.limit*2, e.maxWorker)
				} else {
					e.limit = min(e.limit+e.limit/4, e.maxWorker)
				}
			} else if e.limit > initialConcurrency {
				e.limit -= e.limit / 4
			}
			e.tuneMiss = 0
		}
	}

	missed := r.leak > leakTolerance
	if missed {
		e.tuneMiss++
		// Delivery lags the wire: the flush was already committing when the
		// leak became visible, so the real cliff sits well below the observed
		// window. Record it conservatively and pull the window hard under it.
		cliff := r.window / 2
		if e.cliff == 0 || cliff < e.cliff {
			e.cliff = cliff
		}
		if e.fixedWindow == 0 {
			e.window = e.cliff / 2
			if e.window < minCloseWindow {
				e.window = minCloseWindow
			}
		}
		if e.fixedRate == 0 {
			e.rate /= 2
			if e.rate < autoRateMin {
				e.rate = autoRateMin
			}
		}
		return
	}

	if e.fixedWindow == 0 {
		// Slow growth: the window must stay clearly under the flush point,
		// because everything the flush commits before the abort is billed on
		// the client link even when the application never sees it.
		e.window += e.window / 10
		if e.cliff > 0 {
			if cap := e.cliff * 3 / 5; e.window > cap {
				e.window = cap
			}
		}
		if e.window > maxCloseWindow {
			e.window = maxCloseWindow
		}
		if e.window < minCloseWindow {
			e.window = minCloseWindow
		}
	}
	if e.fixedRate == 0 {
		e.rate *= 1.05
		if e.rate > autoRateMax {
			e.rate = autoRateMax
		}
	}
}

// estimateBurn returns the estimated server-side volume consumed by a cycle.
// Only window time past the measured first-byte delay does any burning, and
// the model is credited only while probes confirm the path is alive.
func (e *engine) estimateBurn(r cycleResult) int64 {
	e.mu.Lock()
	ttfb, ingest, objectSize, lastProbe := e.ttfb, e.ingest, e.objectSize, e.lastProbe
	e.mu.Unlock()

	active := r.window - ttfb
	if active <= 0 {
		// The abort landed before the origin answered: only the request and
		// connection setup were spent server-side.
		return max(r.leak, 2<<10)
	}
	if time.Since(lastProbe) > probeTTL {
		// Probes stopped confirming the path: assume idle instead of burning.
		return max(r.leak, 4<<10)
	}
	burn := int64(ingest * float64(active) / float64(time.Millisecond))
	if burn > cycleBurnCap {
		burn = cycleBurnCap
	}
	if objectSize > 0 && burn > objectSize {
		burn = objectSize
	}
	if burn < r.leak {
		burn = r.leak
	}
	return burn
}

func (e *engine) requestBytes(o origin) []byte {
	path := o.path
	if path == "" {
		path = "/"
	}
	return []byte("GET " + path + " HTTP/1.1\r\nHost: " + o.host + "\r\n" +
		"User-Agent: Mozilla/5.0\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n")
}

// runCycle performs one burn cycle: a small request through the outbound,
// zero application reads, then an abort inside the close window.
func (e *engine) runCycle(ctx context.Context, handler outbound.Handler) cycleResult {
	o := e.nextOrigin()
	request := e.requestBytes(o)

	upReader, upWriter := pipe.New(pipe.WithSizeLimit(64 << 10))
	downReader, downWriter := pipe.New(pipe.WithSizeLimit(4 << 10))
	meter := &meteredWriter{w: downWriter}
	link := &transport.Link{Reader: upReader, Writer: meter}

	sctx := session.ContextWithOutbounds(xctx.ContextWithID(ctx, session.NewID()), []*session.Outbound{{
		Target: net.TCPDestination(parseHost(o.host), net.Port(o.port)),
	}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Dispatch(sctx, link)
	}()

	started := time.Now()
	e.mu.Lock()
	window := e.window
	e.mu.Unlock()
	result := cycleResult{window: window}
	if err := upWriter.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(request)}); err != nil {
		result.failed = true
	}

	// Handlers hand mux sessions off asynchronously, so Dispatch returning is
	// not a cycle-end signal. Only the close window decides when to abort.
	timer := time.NewTimer(window)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}

	if e.abort == abortFIN {
		e.drainFIN(downReader)
		upWriter.Close()
	} else {
		if e.readBytes > 0 {
			e.readSome(downReader, e.readBytes)
		}
		// A close with data still pending on the socket makes the kernel
		// answer with RST, which stops the server side before its pipeline
		// can flush onto the tunnel. Both directions are torn down at once.
		upWriter.Interrupt()
		downReader.Interrupt()
	}
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
	}

	// Everything the outbound handed over crossed the tunnel, whether it was
	// read or not: that is the session's visible bill.
	result.leak = meter.count
	result.billed = int64(len(request)) + result.leak
	if !meter.firstByte.IsZero() {
		result.firstByte = meter.firstByte.Sub(started)
	}
	result.burn = e.estimateBurn(result)
	result.missed = result.leak > leakTolerance
	result.elapsed = time.Since(started)
	return result
}

// runWarmup runs one graceful session before burn cycles. It doubles as a
// liveness probe: it reports whether the origin actually answered.
func (e *engine) runWarmup(ctx context.Context, handler outbound.Handler) bool {
	o := e.nextOrigin()
	request := e.requestBytes(o)

	upReader, upWriter := pipe.New(pipe.WithSizeLimit(64 << 10))
	downReader, downWriter := pipe.New(pipe.WithSizeLimit(8 << 10))
	link := &transport.Link{Reader: upReader, Writer: downWriter}

	sctx := session.ContextWithOutbounds(xctx.ContextWithID(ctx, session.NewID()), []*session.Outbound{{
		Target: net.TCPDestination(parseHost(o.host), net.Port(o.port)),
	}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Dispatch(sctx, link)
	}()

	start := time.Now()
	if err := upWriter.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(request)}); err != nil {
		upWriter.Interrupt()
		downReader.Interrupt()
		return false
	}

	// Enough bytes to prove delivery and measure how fast the origin feeds the
	// server; kept small and rare so the calibration stays a rounding error on
	// the billed footprint.
	var received int64
	var ttfb time.Duration
	var firstAt, lastAt time.Time
	for received < 16<<10 {
		mb, err := downReader.ReadMultiBufferTimeout(2 * time.Second)
		now := time.Now()
		if n := int64(mb.Len()); n > 0 {
			if received == 0 {
				ttfb = now.Sub(start)
				firstAt = now
			}
			received += n
			lastAt = now
		}
		buf.ReleaseMulti(mb)
		if err != nil {
			break
		}
	}
	upWriter.Close()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
	}

	if received == 0 {
		return false
	}
	e.recordTTFB(ttfb)
	duration := lastAt.Sub(firstAt)
	if duration <= 0 {
		duration = time.Since(start) - ttfb
	}
	e.mu.Lock()
	e.lastProbe = time.Now()
	if duration > 0 {
		// Actively reading removes backpressure, so the arrival rate is a real
		// sample of the origin-to-server throughput.
		rate := float64(received) * float64(time.Millisecond) / float64(duration)
		if !e.measured {
			e.ingest = rate
			e.measured = true
		} else {
			e.ingest = (e.ingest + rate*3) / 4
		}
	}
	e.mu.Unlock()
	return true
}

func (e *engine) readSome(r *pipe.Reader, max int64) {
	var read int64
	for read < max {
		mb, err := r.ReadMultiBufferTimeout(10 * time.Millisecond)
		read += int64(mb.Len())
		buf.ReleaseMulti(mb)
		if err != nil {
			break
		}
	}
}

func (e *engine) drainFIN(r *pipe.Reader) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mb, err := r.ReadMultiBufferTimeout(250 * time.Millisecond)
		buf.ReleaseMulti(mb)
		if err != nil {
			break
		}
	}
}

func parseHost(host string) net.Address {
	if addr := net.ParseAddress(host); addr != nil {
		return addr
	}
	return net.DomainAddress(host)
}

type meteredWriter struct {
	w         buf.Writer
	count     int64
	firstByte time.Time
}

func (m *meteredWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if m.count == 0 && mb.Len() > 0 {
		m.firstByte = time.Now()
	}
	m.count += int64(mb.Len())
	return m.w.WriteMultiBuffer(mb)
}
