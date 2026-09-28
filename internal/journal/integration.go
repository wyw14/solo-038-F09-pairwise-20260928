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

// PlateRecord captures a plate change so the press can be restored to the
// exact plate generation after restart.
type PlateRecord struct {
	Plate           string `json:"plate"`
	PlateGeneration uint64 `json:"plate_generation"`
}

// RegisterWindowRecord captures the opening of a register window so its
// generation fence can be restored.
type RegisterWindowRecord struct {
	JobGeneration   uint64  `json:"job_generation"`
	PlateGeneration uint64  `json:"plate_generation"`
	StartMeter      float64 `json:"start_meter"`
}

// DryerStartRecord captures a durable dryer heat request. It proves the
// operator asked for heat; it does NOT prove the dryer is hot.
type DryerStartRecord struct {
	Generation uint64  `json:"generation"`
	Target     float64 `json:"target"`
	Profile    string  `json:"profile"`
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

func PersistJobSnapshot(store *Store, job model.Job, operation model.OperationID, now time.Time) (uint64, error) {
	if err := job.Validate(); err != nil {
		return 0, err
	}
	event, err := model.NewEvent(operation, job, "job_snapshot", job, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistJobCreated(store *Store, job model.Job, operation model.OperationID, now time.Time) (uint64, error) {
	if err := job.Validate(); err != nil {
		return 0, err
	}
	event, err := model.NewEvent(operation, job, "job_created", job, now)
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

func PersistPlate(store *Store, job model.Job, operation model.OperationID, record PlateRecord, now time.Time) (uint64, error) {
	if record.Plate == "" || record.PlateGeneration == 0 {
		return 0, fmt.Errorf("plate record is incomplete")
	}
	event, err := model.NewEvent(operation, job, "plate_changed", record, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistRegisterWindow(store *Store, job model.Job, operation model.OperationID, record RegisterWindowRecord, now time.Time) (uint64, error) {
	if record.JobGeneration == 0 || record.PlateGeneration == 0 {
		return 0, fmt.Errorf("register window record is incomplete")
	}
	event, err := model.NewEvent(operation, job, "register_window_opened", record, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}

func PersistDryerStart(store *Store, job model.Job, operation model.OperationID, record DryerStartRecord, now time.Time) (uint64, error) {
	if record.Generation == 0 || record.Profile == "" || record.Target < 0 {
		return 0, fmt.Errorf("dryer start record is incomplete")
	}
	event, err := model.NewEvent(operation, job, "dryer_started", record, now)
	if err != nil {
		return 0, err
	}
	return store.Append(event)
}
