package journal

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type IntervalRecord struct {
	Interval model.Interval `json:"interval"`
	Proofs   []model.Proof  `json:"proofs"`
	Released bool           `json:"released"`
}

func PersistInterval(store *Store, job model.Job, operation model.OperationID, record IntervalRecord, now time.Time) (uint64, error) {
	if err := record.Interval.Validate(); err != nil {
		return 0, err
	}
	event, err := model.NewEvent(operation, job, "interval_committed", record, now)
	if err != nil {
		return 0, err
	}
	sequence, err := store.Append(event)
	if err != nil {
		return sequence, err
	}
	if !store.State().IsDurable(sequence) {
		return sequence, fmt.Errorf("interval sequence %d is not durable", sequence)
	}
	return sequence, nil
}

func PersistTransition(store *Store, job model.Job, operation model.OperationID, from, to model.JobState, now time.Time) (uint64, error) {
	payload := map[string]any{"from": from, "to": to, "generation": job.Generation}
	event, err := model.NewEvent(operation, job, "job_transitioned", payload, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistFence(store *Store, job model.Job, operation model.OperationID, owner string, now time.Time) (uint64, error) {
	payload := map[string]any{"owner": owner, "roll_generation": job.RollGeneration}
	event, err := model.NewEvent(operation, job, "web_fence_restored", payload, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistJobSnapshot(store *Store, job model.Job, operation model.OperationID, kind string, now time.Time) (uint64, error) {
	if kind != "job_created" && kind != "job_snapshot" {
		return 0, fmt.Errorf("unsupported job snapshot kind %q", kind)
	}
	event, err := model.NewEvent(operation, job, kind, job, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistPlateChange(store *Store, job model.Job, operation model.OperationID, plate string, plateGeneration uint64, now time.Time) (uint64, error) {
	if plate == "" || plateGeneration == 0 {
		return 0, fmt.Errorf("plate change identity is incomplete")
	}
	payload := map[string]any{"plate": plate, "plate_generation": plateGeneration}
	event, err := model.NewEvent(operation, job, "plate_changed", payload, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistDryerStarted(store *Store, job model.Job, operation model.OperationID, target float64, profile string, now time.Time) (uint64, error) {
	if target < 0 || profile == "" {
		return 0, fmt.Errorf("dryer start record is incomplete")
	}
	payload := map[string]any{"target": target, "profile": profile}
	event, err := model.NewEvent(operation, job, "dryer_started", payload, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistRegisterWindow(store *Store, job model.Job, operation model.OperationID, window RegisterWindowRecord, now time.Time) (uint64, error) {
	if window.JobGeneration == 0 || window.PlateGeneration == 0 {
		return 0, fmt.Errorf("register window record is incomplete")
	}
	event, err := model.NewEvent(operation, job, "register_window_opened", window, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}
