package backdraft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	feature_stats "github.com/xtls/xray-core/features/stats"
)

// Server runs connection-abort cycles through an outbound and meters their
// progress toward a server-side volume target.
type Server struct {
	conf   *Config
	engine *engine

	ohm outbound.Manager

	burned feature_stats.Counter
	billed feature_stats.Counter
	cycles feature_stats.Counter
	missed feature_stats.Counter

	burnedTotal atomic.Int64
	billedTotal atomic.Int64
	cyclesTotal atomic.Int64
	inFlight    atomic.Int64
	startedAt   time.Time

	lock    sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewServer creates a new Backdraft server based on the given config.
func NewServer(ctx context.Context, config *Config) (*Server, error) {
	e, err := newEngine(config)
	if err != nil {
		return nil, err
	}
	s := &Server{conf: config, engine: e}
	common.Must(core.RequireFeatures(ctx, func(om outbound.Manager, sm feature_stats.Manager) {
		s.ohm = om
		s.burned, _ = sm.GetOrRegisterCounter("backdraft>>burned")
		s.billed, _ = sm.GetOrRegisterCounter("backdraft>>billed")
		s.cycles, _ = sm.GetOrRegisterCounter("backdraft>>cycles")
		s.missed, _ = sm.GetOrRegisterCounter("backdraft>>missed")
	}))
	return s, nil
}

// Type implements common.HasType.
func (s *Server) Type() interface{} {
	return (*Server)(nil)
}

// Start implements common.Runnable.
func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.running {
		return errors.New("already running")
	}

	target, err := ParseVolume(s.conf.GetTarget())
	if err != nil {
		return errors.New("invalid target").Base(err)
	}
	if target <= 0 {
		return errors.New("target is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.running = true
	s.startedAt = time.Now()

	go s.loop(ctx, target)
	return nil
}

// Close implements common.Runnable.
func (s *Server) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.running {
		return nil
	}
	s.cancel()
	<-s.done
	s.running = false
	return nil
}

func (s *Server) stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.running {
		s.cancel()
	}
}

func (s *Server) loop(ctx context.Context, target int64) {
	defer close(s.done)

	handler := s.handler()
	if handler == nil {
		errors.LogError(ctx, "outbound ", s.engine.outbound, " not found, stopping")
		return
	}

	errors.LogInfo(ctx, "running toward target ", formatVolume(target),
		" through outbound ", handler.Tag())

	if s.engine.warmup && !s.ensureTunnel(ctx, handler) {
		return
	}
	if s.engine.warmup {
		errors.LogInfo(ctx, "warmup complete, tunnel ready")
	}

	var wg sync.WaitGroup
	for i := 0; i < s.engine.maxWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				delay := s.engine.nextDelay()
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
				// Gate on the tuned pool size so the automatic hunt can grow
				// and shrink the number of live cycles.
				if s.inFlight.Add(1) > int64(s.engine.currentLimit()) {
					s.inFlight.Add(-1)
					continue
				}
				ok := s.record(ctx, s.engine.runCycle(ctx, handler), target)
				s.inFlight.Add(-1)
				if !ok {
					return
				}
				// Periodically prove the tunnel still delivers and refresh the
				// first-byte and throughput measurements.
				if s.engine.warmup && s.cyclesTotal.Load()%250 == 0 && !s.ensureTunnel(ctx, handler) {
					return
				}
			}
		}()
	}
	wg.Wait()
}

// ensureTunnel keeps probing until the tunnel answers or the run is cancelled.
func (s *Server) ensureTunnel(ctx context.Context, handler outbound.Handler) bool {
	for {
		if s.engine.runWarmup(ctx, handler) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		errors.LogWarning(ctx, "tunnel probe got no response, retrying in 5s")
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Second):
		}
	}
}

// record folds a cycle into the stats counters and progress log. It returns
// false once the target is reached and the run should wind down.
func (s *Server) record(ctx context.Context, r cycleResult, target int64) bool {
	s.engine.observe(r)

	s.cyclesTotal.Add(1)
	if s.cycles != nil {
		s.cycles.Add(1)
	}
	if r.failed {
		errors.LogWarning(ctx, "cycle failed after ", r.elapsed)
		return true
	}

	burned := s.burnedTotal.Add(r.burn)
	billed := s.billedTotal.Add(r.billed)
	if s.burned != nil {
		s.burned.Add(r.burn)
	}
	if s.billed != nil {
		s.billed.Add(r.billed)
	}
	if r.missed {
		if s.missed != nil {
			s.missed.Add(1)
		}
		errors.LogWarning(ctx, "close-window missed, leak ", formatVolume(r.leak),
			", backing off to ", s.engine.window)
	}

	amp := float64(burned) / float64(max(billed, 1))
	elapsed := time.Since(s.startedAt).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}
	errors.LogInfo(ctx, fmt.Sprintf(
		"cycle burned=~%s billed=%dB amp=%.0fx window=%s ttfb=%s leak=%s progress=%.2f%% speed=%.2fMB/s",
		formatVolume(r.burn), r.billed, amp, r.window, r.firstByte, formatVolume(r.leak),
		float64(burned)*100/float64(target), float64(burned)/elapsed/(1<<20)))

	if burned >= target {
		errors.LogInfo(ctx, "target ", formatVolume(target), " reached after ",
			s.cyclesTotal.Load(), " cycles, stopping")
		s.stop()
		return false
	}
	return true
}

func (s *Server) handler() outbound.Handler {
	if s.engine.outbound != "" {
		return s.ohm.GetHandler(s.engine.outbound)
	}
	return s.ohm.GetDefaultHandler()
}

// Validate checks the config the same way the running engine will interpret it.
func (c *Config) Validate() error {
	_, err := newEngine(c)
	return err
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewServer(ctx, config.(*Config))
	}))
}
