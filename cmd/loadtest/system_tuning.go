package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
)

type sysctlIntFlag struct {
	name string
	key  string
	min  int
	max  int
}

func (f sysctlIntFlag) String() string {
	return ""
}

func (f sysctlIntFlag) Set(value string) error {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", f.name, value, err)
	}
	if n < f.min || n > f.max {
		return fmt.Errorf("%s must be between %d and %d", f.name, f.min, f.max)
	}
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("-%s is currently supported only on macOS", f.name)
	}
	if sysctlValue(f.key) == value {
		return nil
	}

	cmd := exec.Command("sudo", "sysctl", "-w", f.key+"="+value)
	cmd.Stdin = os.Stdin
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("set %s=%s: %w", f.key, value, err)
	}
	if actual := sysctlValue(f.key); actual != value {
		return fmt.Errorf("set %s=%s did not take effect; current value is %s", f.key, value, actual)
	}
	return nil
}

func init() {
	flag.Var(sysctlIntFlag{
		name: "ephemeral-first",
		key:  "net.inet.ip.portrange.first",
		min:  1024,
		max:  65534,
	}, "ephemeral-first", "set macOS ephemeral port range start before the benchmark (requires sudo)")
	flag.Var(sysctlIntFlag{
		name: "somaxconn",
		key:  "kern.ipc.somaxconn",
		min:  1,
		max:  65535,
	}, "somaxconn", "set macOS listen backlog limit before the benchmark (requires sudo)")
}
