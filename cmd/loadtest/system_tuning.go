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

type ephemeralFirstFlag struct{}

func (ephemeralFirstFlag) String() string {
	return ""
}

func (ephemeralFirstFlag) Set(value string) error {
	first, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid ephemeral port start %q: %w", value, err)
	}
	if first < 1024 || first >= 65535 {
		return fmt.Errorf("ephemeral port start must be between 1024 and 65534")
	}
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("-ephemeral-first is currently supported only on macOS")
	}

	key := "net.inet.ip.portrange.first"
	if sysctlValue(key) == value {
		return nil
	}

	cmd := exec.Command("sudo", "sysctl", "-w", key+"="+value)
	cmd.Stdin = os.Stdin
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("set %s=%s: %w", key, value, err)
	}
	if actual := sysctlValue(key); actual != value {
		return fmt.Errorf("set %s=%s did not take effect; current value is %s", key, value, actual)
	}
	return nil
}

func init() {
	flag.Var(ephemeralFirstFlag{}, "ephemeral-first", "set macOS ephemeral port range start before the benchmark (requires sudo)")
}
