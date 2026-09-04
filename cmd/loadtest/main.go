package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type config struct {
	url         string
	connections int
	duration    time.Duration
	interval    time.Duration
	churn       time.Duration
}

type metrics struct {
	connected       atomic.Int64
	connectAttempts atomic.Int64
	connectErr      atomic.Int64
	runtimeErr      atomic.Int64
	sent            atomic.Int64
	received        atomic.Int64
	reconnects      atomic.Int64

	mu         sync.Mutex
	latencies  []time.Duration
	errorKinds map[string]int64
}

type request struct {
	ID     string `json:"id"`
	Action string `json:"action"`
	Params string `json:"params"`
}

type response struct {
	Code   int    `json:"code"`
	Action string `json:"action"`
	ID     string `json:"id"`
	Msg    string `json:"msg"`
}

func (m *metrics) addLatency(d time.Duration) {
	m.mu.Lock()
	m.latencies = append(m.latencies, d)
	m.mu.Unlock()
}

func (m *metrics) addError(kind string, err error) {
	if err == nil {
		return
	}
	key := kind + ": " + err.Error()
	m.mu.Lock()
	if m.errorKinds == nil {
		m.errorKinds = make(map[string]int64)
	}
	m.errorKinds[key]++
	m.mu.Unlock()
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.url, "url", "ws://127.0.0.1:2015/ws", "AQI websocket URL")
	flag.IntVar(&cfg.connections, "connections", 1000, "concurrent websocket connections")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "test duration")
	flag.DurationVar(&cfg.interval, "interval", 2*time.Second, "request interval per connection")
	flag.DurationVar(&cfg.churn, "churn", 0, "random reconnect interval per connection; 0 disables churn")
	flag.Parse()

	if cfg.connections <= 0 || cfg.duration <= 0 || cfg.interval <= 0 || cfg.churn < 0 {
		flag.Usage()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	m := &metrics{}
	started := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runClient(ctx, cfg, id, m)
		}(i)
	}

	printTicker := time.NewTicker(5 * time.Second)
	defer printTicker.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-printTicker.C:
			fmt.Printf("elapsed=%s connected=%d attempts=%d connectErr=%d runtimeErr=%d sent=%d recv=%d reconnects=%d\n",
				time.Since(started).Round(time.Second), m.connected.Load(), m.connectAttempts.Load(), m.connectErr.Load(), m.runtimeErr.Load(), m.sent.Load(), m.received.Load(), m.reconnects.Load())
		case <-done:
			printSummary(cfg, time.Since(started), m)
			return
		}
	}
}

func runClient(ctx context.Context, cfg config, id int, m *metrics) {
	hadSuccessfulConnection := false
	for ctx.Err() == nil {
		m.connectAttempts.Add(1)

		header := http.Header{}
		header.Set("User-Agent", "aqi-bench-loadtest")
		language := "zh"
		if rand.IntN(2) == 0 {
			language = "en"
		}
		url := fmt.Sprintf("%s?platform=bench&appId=bench-%d&clientId=client-%d&lang=%s", cfg.url, id, id, language)
		conn, _, _, err := gws.Dialer{Header: gws.HandshakeHeaderHTTP(header)}.Dial(ctx, url)
		if err != nil {
			m.connectErr.Add(1)
			m.addError("connect", err)
			if !sleepContext(ctx, 250*time.Millisecond) {
				return
			}
			continue
		}

		if hadSuccessfulConnection {
			m.reconnects.Add(1)
		}
		hadSuccessfulConnection = true
		m.connected.Add(1)

		err = runConnection(ctx, conn, cfg, id, m)
		m.connected.Add(-1)
		_ = conn.Close()

		if err != nil && ctx.Err() == nil {
			m.runtimeErr.Add(1)
			m.addError("runtime", err)
		}

		if cfg.churn == 0 || ctx.Err() != nil {
			return
		}
	}
}

func runConnection(ctx context.Context, conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}, cfg config, id int, m *metrics) error {
	// Spread the first send across one full interval so thousands of freshly
	// connected clients do not artificially fire on the same scheduler tick.
	firstDelay := time.Duration(rand.Int64N(int64(cfg.interval)))
	sendTimer := time.NewTimer(firstDelay)
	defer sendTimer.Stop()

	var churn <-chan time.Time
	var churnTimer *time.Timer
	if cfg.churn > 0 {
		jitter := time.Duration(rand.Int64N(int64(cfg.churn)))
		churnTimer = time.NewTimer(cfg.churn + jitter)
		churn = churnTimer.C
		defer churnTimer.Stop()
	}

	sequence := int64(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-churn:
			return nil
		case now := <-sendTimer.C:
			sequence++
			idValue := fmt.Sprintf("%d-%d", id, sequence)
			payload := fmt.Sprintf("%d:%d", id, now.UnixNano())
			body, err := json.Marshal(request{ID: idValue, Action: "bench.echo", Params: payload})
			if err != nil {
				return fmt.Errorf("marshal request: %w", err)
			}

			started := time.Now()
			if err := wsutil.WriteClientText(conn, body); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			m.sent.Add(1)

			data, op, err := wsutil.ReadServerData(conn)
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			if op != gws.OpText {
				return fmt.Errorf("unexpected opcode: %d", op)
			}

			var reply response
			if err := json.Unmarshal(data, &reply); err != nil {
				return fmt.Errorf("unmarshal response: %w", err)
			}
			if reply.Code != 1001 || reply.Action != "bench.echo" || reply.ID != idValue || reply.Msg != "benchmark message" {
				return fmt.Errorf("response mismatch: code=%d action=%q id=%q msg=%q", reply.Code, reply.Action, reply.ID, reply.Msg)
			}

			m.received.Add(1)
			m.addLatency(time.Since(started))
			sendTimer.Reset(cfg.interval)
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func printSummary(cfg config, elapsed time.Duration, m *metrics) {
	m.mu.Lock()
	latencies := append([]time.Duration(nil), m.latencies...)
	errorKinds := make(map[string]int64, len(m.errorKinds))
	for k, v := range m.errorKinds {
		errorKinds[k] = v
	}
	m.mu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Println("\nAQI WebSocket i18n Hot Path")
	fmt.Println("────────────────────────────")
	fmt.Printf("Target           %s\n", cfg.url)
	fmt.Printf("Connections      %d\n", cfg.connections)
	fmt.Printf("Duration         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Connect attempts %d\n", m.connectAttempts.Load())
	fmt.Printf("Connect errors   %d\n", m.connectErr.Load())
	fmt.Printf("Runtime errors   %d\n", m.runtimeErr.Load())
	fmt.Printf("Messages sent    %d\n", m.sent.Load())
	fmt.Printf("Messages recv    %d\n", m.received.Load())
	fmt.Printf("Reconnects       %d\n", m.reconnects.Load())
	if elapsed > 0 {
		fmt.Printf("Throughput       %.1f msg/s\n", float64(m.received.Load())/elapsed.Seconds())
	}
	fmt.Printf("RTT P50          %s\n", percentile(latencies, 0.50))
	fmt.Printf("RTT P95          %s\n", percentile(latencies, 0.95))
	fmt.Printf("RTT P99          %s\n", percentile(latencies, 0.99))

	printErrors(errorKinds)
}

func printErrors(errorKinds map[string]int64) {
	if len(errorKinds) == 0 {
		return
	}

	type entry struct {
		message string
		count   int64
	}
	entries := make([]entry, 0, len(errorKinds))
	for message, count := range errorKinds {
		entries = append(entries, entry{message: message, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count == entries[j].count {
			return entries[i].message < entries[j].message
		}
		return entries[i].count > entries[j].count
	})

	fmt.Println("\nErrors by type")
	fmt.Println("────────────────────────────")
	limit := len(entries)
	if limit > 10 {
		limit = 10
	}
	for i := 0; i < limit; i++ {
		fmt.Printf("%6d  %s\n", entries[i].count, entries[i].message)
	}
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}
