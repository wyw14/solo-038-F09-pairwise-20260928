package journal

import (
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

// Fence is the durable web-owner fence recorded by web_fence_restored events.
type Fence struct {
	Owner          string
	JobGeneration  uint64
	RollGeneration uint64
	At             time.Time
	EventID        model.EventID
}

// PlateEvidence is the durable identity of the plate mounted in the press.
type PlateEvidence struct {
	Plate           string
	PlateGeneration uint64
	JobGeneration   uint64
	At              time.Time
	EventID         model.EventID
}

// RegisterEvidence is the durable opening of the register window.
type RegisterEvidence struct {
	JobGeneration   uint64
	PlateGeneration uint64
	StartMeter      float64
	StartedAt       time.Time
	EventID         model.EventID
}

// DryerEvidence is the durable dryer start request (target/profile), not heat itself.
type DryerEvidence struct {
	JobID      model.JobID
	Generation uint64
	Target     float64
	Profile    string
	At         time.Time
	EventID    model.EventID
}

type RecoveredState struct {
	// Job is the job reconstructed from durable job_created / job_snapshot /
	// job_transitioned events.
	Job    model.Job
	HasJob bool
	// Owner keeps the recovered web owner for callers that predate Fence.
	Owner string
	// Fence is the latest durable web fence.
	Fence    Fence
	HasFence bool
	// Plate / Register / Dryer are the latest durable evidence per component.
	Plate       PlateEvidence
	HasPlate    bool
	Register    RegisterEvidence
	HasRegister bool
	Dryer       DryerEvidence
	HasDryer    bool
	// Intervals are durable committed intervals, in deterministic order. An
	// interval_committed event on disk is by definition released, because the
	// event is only observable after a durable append.
	Intervals      []IntervalRecord
	ReleasedRanges []model.Interval
	// EventCount is the number of event rows read, including duplicates and
	// rows belonging to foreign jobs. Replayed counts unique rows applied.
	// Duplicates counts rows skipped because their event ID was already seen.
	EventCount int
	Replayed   int
	Duplicates int
}

// Replay folds durable events into a recoverable state. It is a pure,
// idempotent fold: replaying the same slice any number of times yields an
// equal RecoveredState, and a duplicated event row never applies twice.
func Replay(events []model.Event) (RecoveredState, error) {
	ordered := append([]model.Event(nil), events...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].At.Before(ordered[right].At)
	})

	seen := map[model.EventID]bool{}
	unique := make([]model.Event, 0, len(ordered))
	for _, event := range ordered {
		if err := event.Validate(); err != nil {
			return RecoveredState{}, err
		}
		if seen[event.ID] {
			continue
		}
		seen[event.ID] = true
		unique = append(unique, event)
	}

	state := RecoveredState{EventCount: len(ordered)}

	// First pass: recover the authoritative job. Component events are only
	// meaningful once the job identity/generation is known.
	for _, event := range unique {
		switch event.Kind {
		case "job_created", "job_snapshot":
			job, err := model.DecodeEventPayload[model.Job](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if err := job.Validate(); err != nil {
				return RecoveredState{}, err
			}
			state.Job = job
			state.HasJob = true
		case "job_transitioned":
			var payload struct {
				From       model.JobState `json:"from"`
				To         model.JobState `json:"to"`
				Generation uint64         `json:"generation"`
			}
			decoded, err := model.DecodeEventPayload[struct {
				From       model.JobState `json:"from"`
				To         model.JobState `json:"to"`
				Generation uint64         `json:"generation"`
			}](event)
			if err != nil {
				return RecoveredState{}, err
			}
			payload = decoded
			if state.HasJob && event.JobID == state.Job.ID && payload.Generation == state.Job.Generation && payload.From == state.Job.State {
				state.Job.State = payload.To
				if err := state.Job.Validate(); err != nil {
					return RecoveredState{}, err
				}
			}
		}
	}
	if state.EventCount > 0 && !state.HasJob {
		return RecoveredState{}, fmt.Errorf("journal has no recoverable job snapshot")
	}

	// Second pass: component evidence belonging to the recovered job.
	for _, event := range unique {
		if state.HasJob && event.JobID != state.Job.ID {
			state.Replayed++
			continue
		}
		switch event.Kind {
		case "job_created", "job_snapshot", "job_transitioned":
			// already folded
		case "web_fence_restored":
			payload, err := model.DecodeEventPayload[struct {
				Owner          string `json:"owner"`
				RollGeneration uint64 `json:"roll_generation"`
			}](event)
			if err != nil {
				return RecoveredState{}, err
			}
			state.Fence = Fence{Owner: payload.Owner, JobGeneration: event.JobGeneration, RollGeneration: payload.RollGeneration, At: event.At, EventID: event.ID}
			state.HasFence = true
			state.Owner = payload.Owner
		case "plate_changed":
			payload, err := model.DecodeEventPayload[PlateRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if payload.Plate == "" || payload.PlateGeneration == 0 {
				return RecoveredState{}, fmt.Errorf("plate evidence is incomplete")
			}
			state.Plate = PlateEvidence{Plate: payload.Plate, PlateGeneration: payload.PlateGeneration, JobGeneration: event.JobGeneration, At: event.At, EventID: event.ID}
			state.HasPlate = true
		case "register_window_opened":
			payload, err := model.DecodeEventPayload[RegisterWindowRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if payload.JobGeneration == 0 || payload.PlateGeneration == 0 {
				return RecoveredState{}, fmt.Errorf("register window evidence is incomplete")
			}
			state.Register = RegisterEvidence{JobGeneration: payload.JobGeneration, PlateGeneration: payload.PlateGeneration, StartMeter: payload.StartMeter, StartedAt: event.At, EventID: event.ID}
			state.HasRegister = true
		case "dryer_started":
			payload, err := model.DecodeEventPayload[DryerStartRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if payload.Generation == 0 || payload.Profile == "" {
				return RecoveredState{}, fmt.Errorf("dryer evidence is incomplete")
			}
			state.Dryer = DryerEvidence{JobID: event.JobID, Generation: payload.Generation, Target: payload.Target, Profile: payload.Profile, At: event.At, EventID: event.ID}
			state.HasDryer = true
		case "interval_committed":
			record, err := model.DecodeEventPayload[IntervalRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if err := record.Interval.Validate(); err != nil {
				return RecoveredState{}, err
			}
			record.Released = true // durable on disk == released
			state.Intervals = append(state.Intervals, record)
			state.ReleasedRanges = append(state.ReleasedRanges, record.Interval)
		default:
			// Unknown event kinds are ignored: newer writers must not break
			// recovery on older binaries.
		}
		state.Replayed++
	}

	sort.Slice(state.Intervals, func(left, right int) bool {
		l, r := state.Intervals[left].Interval, state.Intervals[right].Interval
		if !l.ClosedAt.Equal(r.ClosedAt) {
			return l.ClosedAt.Before(r.ClosedAt)
		}
		return l.StartMeter < r.StartMeter
	})
	state.ReleasedRanges = nil
	for _, record := range state.Intervals {
		state.ReleasedRanges = append(state.ReleasedRanges, record.Interval)
	}
	state.Duplicates = state.EventCount - len(unique)
	return state, nil
}
