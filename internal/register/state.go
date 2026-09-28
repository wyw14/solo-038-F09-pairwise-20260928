package register

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/sensor"
)

type Window struct {
	JobGeneration   uint64    `json:"job_generation"`
	PlateGeneration uint64    `json:"plate_generation"`
	StartMeter      float64   `json:"start_meter"`
	LastMeter       float64   `json:"last_meter"`
	StartedAt       time.Time `json:"started_at"`
	LastAt          time.Time `json:"last_at"`
	Samples         int       `json:"samples"`
	Valid           bool      `json:"valid"`
}

type State struct {
	mu     sync.RWMutex
	window Window
}

func NewState() *State {
	return &State{}
}

func (state *State) Start(jobGeneration, plateGeneration uint64, meter float64, now time.Time) error {
	if jobGeneration == 0 || plateGeneration == 0 {
		return fmt.Errorf("register window generations are required")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.window = Window{JobGeneration: jobGeneration, PlateGeneration: plateGeneration, StartMeter: meter, LastMeter: meter, StartedAt: now.UTC(), LastAt: now.UTC(), Valid: true}
	return nil
}

func (state *State) Add(frame sensor.CameraFrame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.window.Valid || frame.JobGeneration != state.window.JobGeneration || frame.PlateGeneration != state.window.PlateGeneration {
		return fmt.Errorf("register frame generation does not match window")
	}
	if frame.WebMeter < state.window.LastMeter {
		return fmt.Errorf("register frame meter moved backwards")
	}
	state.window.LastMeter = frame.WebMeter
	state.window.LastAt = frame.ReceivedAt
	state.window.Samples++
	return nil
}

func (state *State) Break() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.window.Valid = false
}

func (state *State) Snapshot() Window {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.window
}

// Restore brings back a register window after a restart. The recovered
// window is always marked invalid: the camera dwell is a live guarantee that
// durable evidence cannot confirm, so frames must be re-acquired before the
// window may release anything.
func (state *State) Restore(record Window) error {
	if record.JobGeneration == 0 || record.PlateGeneration == 0 {
		return fmt.Errorf("register window generations are required")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	record.Valid = false
	state.window = record
	return nil
}
