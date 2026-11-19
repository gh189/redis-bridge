package bridge

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

const cgroupDir = "/sys/fs/cgroup/"

type Usage struct {
	CPUPercent    float64 `json:"cpu_percent"`
	CPUCores      float64 `json:"cpu_cores"`
	MemoryUsed    uint64  `json:"memory_used_bytes"`
	MemoryTotal   uint64  `json:"memory_total_bytes"`
	MemoryPercent float64 `json:"memory_percent"`
	DiskUsed      uint64  `json:"disk_used_bytes"`
	DiskTotal     uint64  `json:"disk_total_bytes"`
	DiskPercent   float64 `json:"disk_percent"`
	DiskPath      string  `json:"disk_path"`
}

type sampler struct {
	diskPath string

	mu         sync.Mutex
	lastCPU    float64
	lastAt     time.Time
	cpuPercent float64
}

func (s *sampler) usage() Usage {
	u := Usage{CPUPercent: s.cpu(), CPUCores: cpuCores(), DiskPath: s.diskPath}
	u.MemoryUsed, u.MemoryTotal = memory()
	u.MemoryPercent = percent(u.MemoryUsed, u.MemoryTotal)
	if d, err := disk.Usage(s.diskPath); err == nil {
		u.DiskUsed, u.DiskTotal, u.DiskPercent = d.Used, d.Total, d.UsedPercent
	}
	return u
}

func (s *sampler) cpu() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if !s.lastAt.IsZero() && now.Sub(s.lastAt) < time.Second {
		return s.cpuPercent
	}
	current := cgroupCPUUsage()
	switch {
	case current < 0:
		if host, err := cpu.Percent(0, false); err == nil && len(host) > 0 && !s.lastAt.IsZero() {
			s.cpuPercent = host[0]
		}
	case !s.lastAt.IsZero() && s.lastCPU >= 0:
		pct := (current - s.lastCPU) / (now.Sub(s.lastAt).Seconds() * cpuCores()) * 100
		s.cpuPercent = min(max(pct, 0), 100)
	}
	s.lastCPU, s.lastAt = current, now
	return s.cpuPercent
}

func cgroupCPUUsage() float64 {
	for _, line := range strings.Split(readCgroup("cpu.stat"), "\n") {
		if v, ok := strings.CutPrefix(line, "usage_usec "); ok {
			if usec, err := strconv.ParseFloat(v, 64); err == nil {
				return usec / 1e6
			}
		}
	}
	return -1
}

func cpuCores() float64 {
	if f := strings.Fields(readCgroup("cpu.max")); len(f) == 2 && f[0] != "max" {
		quota, err1 := strconv.ParseFloat(f[0], 64)
		period, err2 := strconv.ParseFloat(f[1], 64)
		if err1 == nil && err2 == nil && period > 0 {
			return quota / period
		}
	}
	return float64(runtime.NumCPU())
}

func memory() (used, total uint64) {
	if vm, err := mem.VirtualMemory(); err == nil {
		used, total = vm.Used, vm.Total
	}
	current, err := strconv.ParseUint(readCgroup("memory.current"), 10, 64)
	if err != nil {
		return used, total
	}
	for _, line := range strings.Split(readCgroup("memory.stat"), "\n") {
		if v, ok := strings.CutPrefix(line, "inactive_file "); ok {
			if inactive, err := strconv.ParseUint(v, 10, 64); err == nil && inactive < current {
				current -= inactive
			}
		}
	}
	if limit, err := strconv.ParseUint(readCgroup("memory.max"), 10, 64); err == nil {
		total = limit
	}
	return current, total
}

func readCgroup(name string) string {
	b, err := os.ReadFile(cgroupDir + name)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func percent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total) * 100
}
