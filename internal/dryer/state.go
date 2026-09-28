package dryer

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type State struct {
	mu          sync.RWMutex
	jobID       string
	jobGen      uint64
	target      float64
	current     float64
	profile     string
	lastUpdated time.Time
	active      bool
}

func NewState() *State {
	return &State{}
}

func (state *State) Start(jobID string, generation uint64, target float64, profile string, now time.Time) error {
	if jobID == "" || generation == 0 || target < 0 || profile == "" {
		return fmt.Errorf("dryer start parameters are incomplete")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.jobID = jobID
	state.jobGen = generation
	state.target = target
	state.profile = profile
	state.current = 0
	state.lastUpdated = now.UTC()
	state.active = true
	return nil
}

func (state *State) Cancel(jobID string, generation uint64) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.jobID != jobID || state.jobGen != generation {
		return false
	}
	state.active = false
	return true
}

func (state *State) Apply(jobID string, generation uint64, temperature float64, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.jobID != jobID || state.jobGen != generation {
		return fmt.Errorf("dryer command is stale")
	}
	state.current = temperature
	state.lastUpdated = now.UTC()
	return nil
}

func (state *State) Snapshot() (string, uint64, float64, float64, string, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.jobID, state.jobGen, state.target, state.current, state.profile, state.active
}

// Restore installs the fenced dryer identity recovered after a restart. The
// heater is always brought back inactive (active=false, current=0): a dryer
// is a thermal actuator and must be re-ramped under operator control rather
// than silently reheating on reboot.
func (state *State) Restore(jobID string, generation uint64, target float64, profile string) error {
	if jobID == "" || generation == 0 || target < 0 || profile == "" {
		return fmt.Errorf("dryer recovery identity is incomplete")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.jobID = jobID
	state.jobGen = generation
	state.target = target
	state.current = 0
	state.profile = profile
	state.lastUpdated = time.Time{}
	state.active = false
	return nil
}

func Ramp(ctx context.Context, _ *State, _ string, _ uint64, target float64, step time.Duration, apply func(float64) error) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		ticker := time.NewTicker(step)
		defer ticker.Stop()
		current := 0.0
		for {
			select {
			case <-ctx.Done():
				result <- nil
				return
			case <-ticker.C:
				current += (target - current) * 0.2
				if err := apply(current); err != nil {
					result <- err
					return
				}
				if current >= target-0.1 {
					result <- nil
					return
				}
			}
		}
	}()
	return result
}
