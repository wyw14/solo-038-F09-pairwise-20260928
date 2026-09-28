package journal

import (
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func testJob() model.Job {
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	job := model.NewJob(now)
	job.RollGeneration = 2
	job.PlateGeneration = 3
	return job
}

func mkEvent(t *testing.T, job model.Job, kind string, payload any, at time.Time) model.Event {
	t.Helper()
	event, err := model.NewEvent(model.NewOperationID(), job, kind, payload, at)
	if err != nil {
		t.Fatalf("build event %s: %v", kind, err)
	}
	return event
}

func TestReplayIdempotentAcrossRepeatedAndDuplicateRows(t *testing.T) {
	job := testJob()
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		mkEvent(t, job, "job_created", job, base),
		mkEvent(t, job, "web_fence_restored", map[string]any{"owner": string(job.ID), "roll_generation": float64(2)}, base.Add(time.Second)),
		mkEvent(t, job, "plate_changed", PlateRecord{Plate: "p1", PlateGeneration: 3}, base.Add(2*time.Second)),
	}

	first, err := Replay(events)
	if err != nil {
		t.Fatalf("first replay: %v", err)
	}
	second, err := Replay(events)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}

	// Same input must fold to an identical state.
	if !jobsEqual(first.Job, second.Job) ||
		first.Owner != second.Owner ||
		first.HasFence != second.HasFence ||
		first.Plate != second.Plate {
		t.Fatalf("repeated replay changed state:\n%+v\n%+v", first, second)
	}

	// Append exact duplicate rows: final state must be unchanged.
	withDupes := append(append([]model.Event{}, events...), events...)
	third, err := Replay(withDupes)
	if err != nil {
		t.Fatalf("duplicate replay: %v", err)
	}
	if third.Duplicates != len(events) {
		t.Fatalf("expected %d duplicates, got %d", len(events), third.Duplicates)
	}
	if !jobsEqual(first.Job, third.Job) || third.Owner != first.Owner || third.Plate != first.Plate {
		t.Fatalf("duplicate replay changed state:\n%+v\n%+v", first, third)
	}
	if third.Replayed != first.Replayed {
		t.Fatalf("unique applied event count changed: %d vs %d", third.Replayed, first.Replayed)
	}
}

func TestReplayFoldsTransitions(t *testing.T) {
	job := testJob()
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	threading, err := job.Transition(model.JobThreading, base)
	if err != nil {
		t.Fatal(err)
	}
	registering, err := threading.Transition(model.JobRegistering, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	printing, err := registering.Transition(model.JobPrinting, base.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	events := []model.Event{
		mkEvent(t, job, "job_created", job, base),
		mkEvent(t, threading, "job_transitioned", map[string]any{"from": "loading", "to": "threading", "generation": float64(1)}, base.Add(time.Second)),
		mkEvent(t, printing, "job_transitioned", map[string]any{"from": "threading", "to": "registering", "generation": float64(1)}, base.Add(2*time.Second)),
		mkEvent(t, printing, "job_transitioned", map[string]any{"from": "registering", "to": "printing", "generation": float64(1)}, base.Add(3*time.Second)),
	}
	recovered, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Job.State != model.JobPrinting {
		t.Fatalf("expected printing, got %s", recovered.Job.State)
	}
}

func TestReplayRequiresJobWhenEventsExist(t *testing.T) {
	job := testJob()
	events := []model.Event{
		mkEvent(t, job, "web_fence_restored", map[string]any{"owner": "x", "roll_generation": float64(2)}, time.Now()),
	}
	if _, err := Replay(events); err == nil {
		t.Fatal("expected error when journal has component events but no job")
	}
}

func TestReplayCollectsComponentEvidence(t *testing.T) {
	job := testJob()
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		mkEvent(t, job, "job_created", job, base),
		mkEvent(t, job, "register_window_opened", RegisterWindowRecord{JobGeneration: 1, PlateGeneration: 3, StartMeter: 5}, base.Add(time.Second)),
		mkEvent(t, job, "dryer_started", DryerStartRecord{Generation: 1, Target: 100, Profile: "production"}, base.Add(2*time.Second)),
		mkEvent(t, job, "interval_committed", IntervalRecord{
			Interval: model.Interval{JobID: job.ID, JobGeneration: 1, RollGeneration: 2, StartMeter: 0, EndMeter: 10, OpenedAt: base, ClosedAt: base.Add(3 * time.Second)},
			Released: false,
		}, base.Add(3*time.Second)),
	}
	got, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasRegister || got.Register.PlateGeneration != 3 {
		t.Fatalf("register evidence missing: %+v", got.Register)
	}
	if !got.HasDryer || got.Dryer.Target != 100 {
		t.Fatalf("dryer evidence missing: %+v", got.Dryer)
	}
	if len(got.Intervals) != 1 || !got.Intervals[0].Released {
		t.Fatalf("interval should be marked released from durable row: %+v", got.Intervals)
	}
}

func jobsEqual(a, b model.Job) bool {
	return a.ID == b.ID && a.State == b.State && a.Generation == b.Generation &&
		a.RollGeneration == b.RollGeneration && a.PlateGeneration == b.PlateGeneration
}
