package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
)

type Cancellation struct {
	JobID       model.JobID `json:"job_id"`
	Generation  uint64      `json:"generation"`
	CancelledAt time.Time   `json:"cancelled_at"`
	Components  []string    `json:"components"`
}

func (runtime *Runtime) CancelCurrent(ctx context.Context) (Cancellation, error) {
	current, ok := runtime.jobs.Current()
	if !ok {
		return Cancellation{}, fmt.Errorf("there is no current job")
	}
	select {
	case <-ctx.Done():
		return Cancellation{}, ctx.Err()
	default:
	}
	if err := runtime.jobControl.Cancel(current, time.Now()); err != nil {
		return Cancellation{}, err
	}
	return Cancellation{JobID: current.ID, Generation: current.Generation, CancelledAt: time.Now().UTC(), Components: []string{"job", "dryer", "web"}}, nil
}

func (runtime *Runtime) SaveSnapshot() error {
	current, ok := runtime.jobs.Current()
	if !ok {
		return fmt.Errorf("there is no current job")
	}
	owner, jobGeneration, rollGeneration, threaded, meter := runtime.web.Snapshot()
	plate, plateGeneration, color, phases := runtime.press.Snapshot()
	window := runtime.register.Snapshot()
	dryerJobID, dryerGeneration, dryerTarget, dryerCurrent, dryerProfile, dryerActive := runtime.dryer.Snapshot()
	values := map[string]any{
		"job":                    current,
		"web_owner":              owner,
		"web_job_generation":     jobGeneration,
		"web_roll_generation":    rollGeneration,
		"web_threaded":           threaded,
		"web_meter":              meter,
		"press_plate":            plate,
		"press_plate_generation": plateGeneration,
		"press_color":            color,
		"press_phases":           phases,
		"register_window": journal.RegisterWindowRecord{
			JobGeneration:   window.JobGeneration,
			PlateGeneration: window.PlateGeneration,
			StartMeter:      window.StartMeter,
			LastMeter:       window.LastMeter,
			Samples:         window.Samples,
		},
		"dryer_job_id":      dryerJobID,
		"dryer_generation":  dryerGeneration,
		"dryer_target":      dryerTarget,
		"dryer_profile":     dryerProfile,
		"dryer_temperature": dryerCurrent,
		"dryer_active":      dryerActive,
	}
	return runtimeSaveSnapshot(runtime.config.DataDir, values)
}
