package register

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/sensor"
)

type Coordinator struct {
	mu        sync.Mutex
	state     *State
	journal   *journal.Store
	released  map[model.JobID]journal.IntervalRecord
	lastError error
}

func NewCoordinator(state *State, store *journal.Store) *Coordinator {
	return &Coordinator{state: state, journal: store, released: map[model.JobID]journal.IntervalRecord{}}
}

func (coordinator *Coordinator) AcceptFrame(frame sensor.CameraFrame, maximumGap time.Duration) error {
	window := coordinator.state.Snapshot()
	if !window.Valid {
		return fmt.Errorf("register window is not active")
	}
	if frame.JobGeneration != window.JobGeneration || frame.PlateGeneration != window.PlateGeneration {
		return fmt.Errorf("camera frame does not belong to active register generation")
	}
	if frame.ReceivedAt.Sub(window.LastAt) > maximumGap {
		coordinator.state.Break()
		return fmt.Errorf("camera gap exceeded register dwell")
	}
	return coordinator.state.Add(frame)
}

func (coordinator *Coordinator) Stable(minimum int) bool {
	window := coordinator.state.Snapshot()
	return window.Valid && window.Samples >= minimum
}

func (coordinator *Coordinator) Release(job model.Job, operation model.OperationID, proofs []model.Proof, now time.Time) error {
	window := coordinator.state.Snapshot()
	if !window.Valid || window.Samples == 0 {
		return fmt.Errorf("register window is incomplete")
	}
	interval := model.Interval{JobID: job.ID, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, StartMeter: window.StartMeter, EndMeter: window.LastMeter, OpenedAt: window.StartedAt, ClosedAt: now.UTC()}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	// The release only becomes true once its event is durable: persist the
	// released record so a power loss afterwards still proves the interval is
	// safe to advance over.
	record := journal.IntervalRecord{Interval: interval, Proofs: proofs, Released: true}
	if _, err := journal.PersistInterval(coordinator.journal, job, operation, record, now); err != nil {
		coordinator.lastError = err
		coordinator.released[job.ID] = journal.IntervalRecord{Interval: interval, Proofs: proofs, Released: false}
		return err
	}
	coordinator.released[job.ID] = record
	return nil
}

func (coordinator *Coordinator) CanAdvance(job model.Job) bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	record, ok := coordinator.released[job.ID]
	return ok && record.Released && record.Interval.JobGeneration == job.Generation && coordinator.lastError == nil
}

func (coordinator *Coordinator) LastError() error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.lastError
}

// RestoreReleased rebuilds the release ledger from durable interval events
// during restart recovery. Only intervals belonging to the recovered job and
// its current generations are restored, so stale releases can never unlock
// a newer roll/plate generation.
func (coordinator *Coordinator) RestoreReleased(job model.Job, intervals []model.Interval) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	for _, interval := range intervals {
		if interval.JobID != job.ID || interval.JobGeneration != job.Generation || interval.RollGeneration != job.RollGeneration {
			continue
		}
		if existing, ok := coordinator.released[job.ID]; ok && existing.Released {
			continue
		}
		coordinator.released[job.ID] = journal.IntervalRecord{Interval: interval, Released: true}
	}
}
