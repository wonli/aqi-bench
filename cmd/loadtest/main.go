package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const dashboardLines = 40

type config struct {
	url         string
	connections int
	connectRate int
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

type systemInfo struct {
	osArch         string
	cpu            string
	gomaxprocs     string
	nofileSoft     string
	nofileHard     string
	maxfiles       string
	maxfilesProc   string
	somaxconn      string
	ephemeralPorts string
}

type processInfo struct {
	goroutines string
	osThreads  string
	openFDs    string
	rss        string
}

type connectedClient struct {
	id       int
	language string
	conn     net.Conn
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
	key := kind + ": " + classifyError(err)
	m.mu.Lock()
	if m.errorKinds == nil {
		m.errorKinds = make(map[string]int64)
	}
	m.errorKinds[key]++
	m.mu.Unlock()
}

func classifyError(err error) string {
	if err == nil {
		return "unknown"
	}
	if err == context.Canceled {
		return "canceled"
	}
	if err == context.DeadlineExceeded {
		return "timeout"
	}
	if err == io.EOF {
		return "EOF"
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return "timeout"
	}

	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "timeout"), strings.Contains(message, "deadline exceeded"):
		return "timeout"
	case strings.Contains(message, "connection refused"):
		return "connection refused"
	case strings.Contains(message, "cannot assign requested address"), strings.Contains(message, "can't assign requested address"):
		return "address unavailable"
	case strings.Contains(message, "too many open files"):
		return "too many open files"
	case strings.Contains(message, "no buffer space available"):
		return "no buffer space"
	case strings.Contains(message, "connection reset"):
		return "connection reset"
	case strings.Contains(message, "broken pipe"):
		return "broken pipe"
	case strings.Contains(message, "unexpected eof"):
		return "unexpected EOF"
	case strings.Contains(message, "eof"):
		return "EOF"
	default:
		return err.Error()
	}
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.url, "url", "ws://127.0.0.1:2015/ws", "AQI websocket URL")
	flag.IntVar(&cfg.connections, "connections", 1000, "concurrent websocket connections")
	flag.IntVar(&cfg.connectRate, "connect-rate", 500, "maximum dial attempts per second; 0 disables throttling")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "benchmark duration after all connections are established")
	flag.DurationVar(&cfg.interval, "interval", 2*time.Second, "request interval per connection")
	flag.DurationVar(&cfg.churn, "churn", 0, "random reconnect interval per connection; 0 disables churn")
	flag.Parse()

	if cfg.connections <= 0 || cfg.connectRate < 0 || cfg.duration <= 0 || cfg.interval <= 0 || cfg.churn < 0 {
		flag.Usage()
		return
	}

	env := collectSystemInfo()
	m := &metrics{}
	printTicker := time.NewTicker(time.Second)
	defer printTicker.Stop()

	setupCtx, cancelSetup := context.WithCancel(context.Background())
	defer cancelSetup()
	dialGate := newDialGate(setupCtx, cfg.connectRate)
	ready := make(chan connectedClient, cfg.connections)

	for i := 0; i < cfg.connections; i++ {
		go func(id int) {
			client, ok := connectClient(setupCtx, cfg, id, m, dialGate)
			if ok {
				ready <- client
			}
		}(i)
	}

	clients := make([]connectedClient, 0, cfg.connections)
	renderDashboard(env, cfg, "connecting", 0, m, false)
	for len(clients) < cfg.connections {
		select {
		case client := <-ready:
			clients = append(clients, client)
		case <-printTicker.C:
			renderDashboard(env, cfg, "connecting", 0, m, false)
		}
	}
	cancelSetup()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()
	started := time.Now()

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client connectedClient) {
			defer wg.Done()
			runEstablishedClient(ctx, cfg, client, m)
		}(client)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	renderDashboard(env, cfg, "benchmarking", 0, m, false)
	for {
		select {
		case <-printTicker.C:
			renderDashboard(env, cfg, "benchmarking", time.Since(started), m, false)
		case <-ctx.Done():
			<-done
			renderDashboard(env, cfg, "complete", cfg.duration, m, true)
			return
		case <-done:
			renderDashboard(env, cfg, "complete", time.Since(started), m, true)
			return
		}
	}
}

func newDialGate(ctx context.Context, rate int) <-chan struct{} {
	if rate <= 0 {
		return nil
	}

	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	gate := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(gate)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case gate <- struct{}{}:
				default:
				}
			}
		}
	}()
	return gate
}

func waitDialGate(ctx context.Context, gate <-chan struct{}) bool {
	if gate == nil {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case _, ok := <-gate:
		return ok
	}
}

func connectClient(ctx context.Context, cfg config, id int, m *metrics, gate <-chan struct{}) (connectedClient, bool) {
	languages := [...]string{"zh", "en", "ja"}
	language := languages[rand.IntN(len(languages))]
	header := http.Header{}
	header.Set("User-Agent", "aqi-bench-loadtest")
	url := fmt.Sprintf("%s?platform=bench&appId=bench-%d&clientId=client-%d&lang=%s", cfg.url, id, id, language)

	for ctx.Err() == nil {
		if !waitDialGate(ctx, gate) {
			return connectedClient{}, false
		}
		m.connectAttempts.Add(1)

		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, _, _, err := gws.Dialer{Header: gws.HandshakeHeaderHTTP(header)}.Dial(dialCtx, url)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return connectedClient{}, false
			}
			m.connectErr.Add(1)
			m.addError("connect", err)
			continue
		}

		m.connected.Add(1)
		return connectedClient{id: id, language: language, conn: conn}, true
	}
	return connectedClient{}, false
}

func collectSystemInfo() systemInfo {
	info := systemInfo{
		osArch:     fmt.Sprintf("%s / %s", runtime.GOOS, runtime.GOARCH),
		cpu:        fmt.Sprintf("%d logical", runtime.NumCPU()),
		gomaxprocs: fmt.Sprintf("%d", runtime.GOMAXPROCS(0)),
		nofileSoft: commandOutput("sh", "-c", "ulimit -Sn"),
		nofileHard: commandOutput("sh", "-c", "ulimit -Hn"),
	}

	switch runtime.GOOS {
	case "darwin":
		info.maxfiles = sysctlValue("kern.maxfiles")
		info.maxfilesProc = sysctlValue("kern.maxfilesperproc")
		info.somaxconn = sysctlValue("kern.ipc.somaxconn")
		first := sysctlValue("net.inet.ip.portrange.first")
		last := sysctlValue("net.inet.ip.portrange.last")
		if first != "" && last != "" {
			info.ephemeralPorts = first + "-" + last
		}
	case "linux":
		info.somaxconn = sysctlValue("net.core.somaxconn")
		if ports := sysctlValue("net.ipv4.ip_local_port_range"); ports != "" {
			info.ephemeralPorts = strings.Join(strings.Fields(ports), "-")
		}
	}
	return info
}

func collectProcessInfo() processInfo {
	return processInfo{
		goroutines: fmt.Sprintf("%d", runtime.NumGoroutine()),
		osThreads:  "external only",
		openFDs:    "external only",
		rss:        "external only",
	}
}

func renderDashboard(env systemInfo, cfg config, phase string, elapsed time.Duration, m *metrics, final bool) {
	if elapsed > cfg.duration {
		elapsed = cfg.duration
	}

	process := collectProcessInfo()
	m.mu.Lock()
	errorKinds := make(map[string]int64, len(m.errorKinds))
	for k, v := range m.errorKinds {
		errorKinds[k] = v
	}
	var latencies []time.Duration
	if final {
		latencies = append([]time.Duration(nil), m.latencies...)
	}
	m.mu.Unlock()

	if final {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	}

	if phase != "connecting" || m.connected.Load() > 0 {
		fmt.Printf("\033[%dA", dashboardLines)
	}

	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(m.received.Load()) / elapsed.Seconds()
	}

	printDashboardLine("AQI WebSocket Benchmark")
	printDashboardLine("────────────────────────────")
	printMetric("OS / Arch", env.osArch)
	printMetric("CPU", env.cpu)
	printMetric("GOMAXPROCS", env.gomaxprocs)
	printMetric("NOFILE soft", env.nofileSoft)
	printMetric("NOFILE hard", env.nofileHard)
	printMetric("kern.maxfiles", env.maxfiles)
	printMetric("maxfiles/proc", env.maxfilesProc)
	printMetric("somaxconn", env.somaxconn)
	printMetric("Ephemeral ports", env.ephemeralPorts)
	printMetric("Goroutines", process.goroutines)
	printMetric("OS threads", process.osThreads)
	printMetric("Open FDs", process.openFDs)
	printMetric("RSS", process.rss)
	printMetric("Phase", phase)
	printMetric("Target", cfg.url)
	printMetric("Connections", fmt.Sprintf("%d", cfg.connections))
	printMetric("Connect rate", connectRateLabel(cfg.connectRate))
	if phase == "connecting" {
		printMetric("Duration", fmt.Sprintf("waiting / %s", cfg.duration))
	} else {
		printMetric("Duration", fmt.Sprintf("%s / %s", elapsed.Round(time.Second), cfg.duration))
	}
	printMetric("Connected", fmt.Sprintf("%d", m.connected.Load()))
	printMetric("Connect attempts", fmt.Sprintf("%d", m.connectAttempts.Load()))
	printMetric("Connect errors", fmt.Sprintf("%d", m.connectErr.Load()))
	printMetric("Runtime errors", fmt.Sprintf("%d", m.runtimeErr.Load()))
	printMetric("Messages sent", fmt.Sprintf("%d", m.sent.Load()))
	printMetric("Messages recv", fmt.Sprintf("%d", m.received.Load()))
	printMetric("Reconnects", fmt.Sprintf("%d", m.reconnects.Load()))
	printMetric("Throughput", fmt.Sprintf("%.1f msg/s", throughput))
	if final {
		printMetric("RTT P50", percentile(latencies, 0.50).String())
		printMetric("RTT P95", percentile(latencies, 0.95).String())
		printMetric("RTT P99", percentile(latencies, 0.99).String())
	} else {
		printMetric("RTT P50", "measured at end")
		printMetric("RTT P95", "measured at end")
		printMetric("RTT P99", "measured at end")
	}
	printDashboardLine("")
	printDashboardLine("Errors by type")
	printDashboardLine("────────────────────────────")
	printErrorRows(errorKinds, 7)
}

func connectRateLabel(rate int) string {
	if rate <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d/s", rate)
}

func printDashboardLine(value string) {
	fmt.Printf("\033[2K%s\n", value)
}

func printMetric(label, value string) {
	fmt.Printf("\033[2K%-17s%s\n", label, value)
}

func printErrorRows(errorKinds map[string]int64, rows int) {
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

	for i := 0; i < rows; i++ {
		if i < len(entries) {
			printDashboardLine(fmt.Sprintf("%6d  %s", entries[i].count, entries[i].message))
		} else {
			printDashboardLine("")
		}
	}
}

func sysctlValue(key string) string {
	return commandOutput("sysctl", "-n", key)
}

func commandOutput(name string, args ...string) string {
	output, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func runEstablishedClient(ctx context.Context, cfg config, client connectedClient, m *metrics) {
	benchmarkDeadline, _ := ctx.Deadline()
	for {
		_ = client.conn.SetDeadline(benchmarkDeadline)
		err := runConnection(ctx, client.conn, cfg, client.id, client.language, m)
		m.connected.Add(-1)
		_ = client.conn.Close()

		if err != nil && !benchmarkEnded(ctx, benchmarkDeadline) {
			m.runtimeErr.Add(1)
			m.addError("runtime", err)
		}
		if cfg.churn == 0 || benchmarkEnded(ctx, benchmarkDeadline) {
			return
		}

		m.reconnects.Add(1)
		next, ok := connectClient(ctx, cfg, client.id, m, nil)
		if !ok {
			return
		}
		client = next
	}
}

func benchmarkEnded(ctx context.Context, deadline time.Time) bool {
	return ctx.Err() != nil || (!deadline.IsZero() && !time.Now().Before(deadline))
}

func runConnection(ctx context.Context, conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}, cfg config, id int, language string, m *metrics) error {
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

	expectedMsg := map[string]string{
		"zh": "benchmark message",
		"en": "benchmark message translated",
		"ja": "ベンチマークメッセージ",
	}[language]

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
			if reply.Code != 1001 || reply.Action != "bench.echo" || reply.ID != idValue || reply.Msg != expectedMsg {
				return fmt.Errorf("response mismatch: lang=%s code=%d action=%q id=%q msg=%q expected=%q", language, reply.Code, reply.Action, reply.ID, reply.Msg, expectedMsg)
			}

			m.received.Add(1)
			m.addLatency(time.Since(started))
			sendTimer.Reset(cfg.interval)
		}
	}
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}
