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
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const dashboardLines = 39

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
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "test duration")
	flag.DurationVar(&cfg.interval, "interval", 2*time.Second, "request interval per connection")
	flag.DurationVar(&cfg.churn, "churn", 0, "random reconnect interval per connection; 0 disables churn")
	flag.Parse()

	if cfg.connections <= 0 || cfg.duration <= 0 || cfg.interval <= 0 || cfg.churn < 0 {
		flag.Usage()
		return
	}

	env := collectSystemInfo()
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

	printTicker := time.NewTicker(1 * time.Second)
	defer printTicker.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	renderDashboard(env, cfg, 0, m, false)
	for {
		select {
		case <-printTicker.C:
			renderDashboard(env, cfg, time.Since(started), m, false)
		case <-ctx.Done():
			<-done
			renderDashboard(env, cfg, cfg.duration, m, true)
			return
		case <-done:
			renderDashboard(env, cfg, time.Since(started), m, true)
			return
		}
	}
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
	info := processInfo{
		goroutines: fmt.Sprintf("%d", runtime.NumGoroutine()),
	}

	fdPath := "/dev/fd"
	if runtime.GOOS == "linux" {
		fdPath = "/proc/self/fd"
	}
	if entries, err := os.ReadDir(fdPath); err == nil {
		info.openFDs = fmt.Sprintf("%d", len(entries))
	}

	pid := strconv.Itoa(os.Getpid())
	var output string
	switch runtime.GOOS {
	case "darwin":
		output = commandOutput("ps", "-o", "thcount=", "-o", "rss=", "-p", pid)
	case "linux":
		output = commandOutput("ps", "-o", "nlwp=", "-o", "rss=", "-p", pid)
	}

	fields := strings.Fields(output)
	if len(fields) >= 2 {
		info.osThreads = fields[0]
		if rssKiB, err := strconv.ParseFloat(fields[1], 64); err == nil {
			info.rss = fmt.Sprintf("%.1f MiB", rssKiB/1024)
		}
	}
	return info
}

func renderDashboard(env systemInfo, cfg config, elapsed time.Duration, m *metrics, final bool) {
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

	if elapsed > 0 {
		if final {
			fmt.Printf("\033[%dA", dashboardLines)
		} else if elapsed >= time.Second {
			fmt.Printf("\033[%dA", dashboardLines)
		}
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
	printMetric("Target", cfg.url)
	printMetric("Connections", fmt.Sprintf("%d", cfg.connections))
	printMetric("Duration", fmt.Sprintf("%s / %s", elapsed.Round(time.Second), cfg.duration))
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

func runClient(ctx context.Context, cfg config, id int, m *metrics) {
	hadSuccessfulConnection := false
	for ctx.Err() == nil {
		m.connectAttempts.Add(1)

		header := http.Header{}
		header.Set("User-Agent", "aqi-bench-loadtest")
		languages := [...]string{"zh", "en", "ja"}
		language := languages[rand.IntN(len(languages))]
		url := fmt.Sprintf("%s?platform=bench&appId=bench-%d&clientId=client-%d&lang=%s", cfg.url, id, id, language)
		conn, _, _, err := gws.Dialer{Header: gws.HandshakeHeaderHTTP(header)}.Dial(ctx, url)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
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

		stopClose := context.AfterFunc(ctx, func() {
			_ = conn.Close()
		})
		err = runConnection(ctx, conn, cfg, id, language, m)
		stopClose()
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

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}
