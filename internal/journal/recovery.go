package journal

import (
	"fmt"
	"sort"

	"github.com/wyw14/solo-038/internal/model"
)

// RecoveredState is the result of replaying the durable event log. Every
// field is keyed by the fence generations carried on the events, so a
// restart can tell which generation each component was last confirmed at.
type RecoveredState struct {
	Job               model.Job
	Owner             string
	OwnerJobGen       uint64
	OwnerRollGen      uint64
	Plate             string
	PlateGeneration   uint64
	DryerJobID        string
	DryerGeneration   uint64
	DryerTarget       float64
	DryerProfile      string
	RegisterWindow    *RegisterWindowRecord
	ReleasedRanges    []model.Interval
	EventCount        int
	UniqueEventCount  int
	DuplicatesSkipped int
}

type RegisterWindowRecord struct {
	JobGeneration   uint64  `json:"job_generation"`
	PlateGeneration uint64  `json:"plate_generation"`
	StartMeter      float64 `json:"start_meter"`
	LastMeter       float64 `json:"last_meter"`
	Samples         int     `json:"samples"`
}

type transitionPayload struct {
	From       model.JobState `json:"from"`
	To         model.JobState `json:"to"`
	Generation uint64         `json:"generation"`
}

// Replay folds durable events into a RecoveredState. Replaying the same log
// twice — or a log that contains the same event more than once — yields the
// same state: events are deduplicated by ID and interval releases are
// deduplicated by their fence identity.
func Replay(events []model.Event) (RecoveredState, error) {
	ordered := append([]model.Event(nil), events...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].At.Before(ordered[right].At)
	})
	state := RecoveredState{}
	seen := map[model.EventID]bool{}
	released := map[string]bool{}
	for _, event := range ordered {
		if err := event.Validate(); err != nil {
			return RecoveredState{}, err
		}
		state.EventCount++
		if seen[event.ID] {
			state.DuplicatesSkipped++
			continue
		}
		seen[event.ID] = true
		state.UniqueEventCount++
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
		case "job_transitioned":
			payload, err := model.DecodeEventPayload[transitionPayload](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if state.Job.ID.Valid() && event.JobID == state.Job.ID && payload.Generation == state.Job.Generation {
				state.Job.State = payload.To
			}
		case "web_fence_restored":
			payload, err := model.DecodeEventPayload[map[string]any](event)
			if err != nil {
				return RecoveredState{}, err
			}
			owner, _ := payload["owner"].(string)
			if owner == "" {
				return RecoveredState{}, fmt.Errorf("web fence event has no owner")
			}
			state.Owner = owner
			state.OwnerJobGen = event.JobGeneration
			state.OwnerRollGen = event.RollGeneration
		case "plate_changed":
			payload, err := model.DecodeEventPayload[map[string]any](event)
			if err != nil {
				return RecoveredState{}, err
			}
			plate, _ := payload["plate"].(string)
			generation := payloadUint(payload, "plate_generation")
			if plate == "" || generation == 0 {
				return RecoveredState{}, fmt.Errorf("plate change event is incomplete")
			}
			state.Plate = plate
			state.PlateGeneration = generation
		case "dryer_started":
			payload, err := model.DecodeEventPayload[map[string]any](event)
			if err != nil {
				return RecoveredState{}, err
			}
			profile, _ := payload["profile"].(string)
			target := payloadFloat(payload, "target")
			if profile == "" || target < 0 {
				return RecoveredState{}, fmt.Errorf("dryer start event is incomplete")
			}
			state.DryerJobID = string(event.JobID)
			state.DryerGeneration = event.JobGeneration
			state.DryerTarget = target
			state.DryerProfile = profile
		case "register_window_opened":
			record, err := model.DecodeEventPayload[RegisterWindowRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if record.JobGeneration == 0 || record.PlateGeneration == 0 {
				return RecoveredState{}, fmt.Errorf("register window event is incomplete")
			}
			window := record
			state.RegisterWindow = &window
		case "interval_committed":
			record, err := model.DecodeEventPayload[IntervalRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			if !record.Released {
				continue
			}
			key := intervalKey(record.Interval)
			if released[key] {
				continue
			}
			released[key] = true
			state.ReleasedRanges = append(state.ReleasedRanges, record.Interval)
		}
	}
	if state.EventCount > 0 && !state.Job.ID.Valid() {
		return RecoveredState{}, fmt.Errorf("journal has events but no recoverable job snapshot")
	}
	return state, nil
}

func intervalKey(interval model.Interval) string {
	return fmt.Sprintf("%s/%d/%d/%.6f/%.6f", interval.JobID, interval.JobGeneration, interval.RollGeneration, interval.StartMeter, interval.EndMeter)
}

func payloadUint(payload map[string]any, key string) uint64 {
	value, ok := payload[key].(float64)
	if !ok || value < 0 {
		return 0
	}
	return uint64(value)
}

func payloadFloat(payload map[string]any, key string) float64 {
	value, _ := payload[key].(float64)
	return value
}
