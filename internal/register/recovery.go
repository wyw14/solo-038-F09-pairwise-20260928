package register

import (
	"fmt"
	"time"
)

// Recovery is the fenced register-window state that can be restored after a
// restart. The window is restored as valid but with zero samples: camera
// samples gathered before the crash cannot be trusted until fresh frames
// confirm the web is still aligned.
type Recovery struct {
	JobGeneration   uint64
	PlateGeneration uint64
	StartMeter      float64
	StartedAt       time.Time
}

// Restore reopens the register window from durable evidence. It never
// replays historical samples; the operator must wait for fresh camera frames
// before releasing product, so Samples starts at zero.
func Restore(state *State, recovery Recovery) error {
	if recovery.JobGeneration == 0 || recovery.PlateGeneration == 0 {
		return fmt.Errorf("register recovery fence is incomplete")
	}
	startedAt := recovery.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.window = Window{JobGeneration: recovery.JobGeneration, PlateGeneration: recovery.PlateGeneration, StartMeter: recovery.StartMeter, LastMeter: recovery.StartMeter, StartedAt: startedAt, LastAt: startedAt, Valid: true}
	return nil
}
