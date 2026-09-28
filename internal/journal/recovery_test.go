package journal

import (
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func newTestEvent(t *testing.T, kind string, payload any, jobGen, rollGen uint64, at time.Time) model.Event {
	t.Helper()
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: jobGen, RollGeneration: rollGen, PlateGeneration: 1, CreatedAt: at, UpdatedAt: at}
	event, err := model.NewEvent(model.NewOperationID(), job, kind, payload, at)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	return event
}

func TestReplayDeduplicatesRepeatedEvents(t *testing.T) {
	at := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 1, RollGeneration: 2, PlateGeneration: 1, CreatedAt: at, UpdatedAt: at}
	jobEvent := newTestEvent(t, "job_created", job, 1, 2, at)
	// A real journal could contain the same durable event twice after a crash
	// mid-append retry; replay must treat it as one.
	events := []model.Event{jobEvent, jobEvent, jobEvent}

	first, err := Replay(events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if first.UniqueEventCount != 1 || first.DuplicatesSkipped != 2 || first.EventCount != 3 {
		t.Fatalf("unexpected counts: %+v", first)
	}
	second, err := Replay(events)
	if err != nil {
		t.Fatalf("replay again: %v", err)
	}
	if second.UniqueEventCount != first.UniqueEventCount || second.Job != first.Job {
		t.Fatalf("repeated replay changed the result")
	}
}

func TestReplayRecoversComponentGenerations(t *testing.T) {
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 3, RollGeneration: 2, PlateGeneration: 4, CreatedAt: base, UpdatedAt: base}
	jobEvent, _ := model.NewEvent(model.NewOperationID(), job, "job_snapshot", job, base)
	fenceEvent, _ := model.NewEvent(model.NewOperationID(), job, "web_fence_restored", map[string]any{"owner": "line-a", "roll_generation": 2}, base.Add(time.Second))
	plateEvent, _ := model.NewEvent(model.NewOperationID(), job, "plate_changed", map[string]any{"plate": "p4", "plate_generation": 4}, base.Add(2*time.Second))
	dryerEvent, _ := model.NewEvent(model.NewOperationID(), job, "dryer_started", map[string]any{"target": 95.0, "profile": "production"}, base.Add(3*time.Second))
	windowEvent, _ := model.NewEvent(model.NewOperationID(), job, "register_window_opened", RegisterWindowRecord{JobGeneration: 3, PlateGeneration: 4, StartMeter: 0, LastMeter: 12, Samples: 6}, base.Add(4*time.Second))
	interval := model.Interval{JobID: job.ID, JobGeneration: 3, RollGeneration: 2, StartMeter: 0, EndMeter: 12, OpenedAt: base, ClosedAt: base.Add(5 * time.Second)}
	releaseRecord := IntervalRecord{Interval: interval, Released: true}
	releaseEvent, _ := model.NewEvent(model.NewOperationID(), job, "interval_committed", releaseRecord, base.Add(5*time.Second))

	recovered, err := Replay([]model.Event{jobEvent, fenceEvent, plateEvent, dryerEvent, windowEvent, releaseEvent})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if recovered.Owner != "line-a" || recovered.OwnerJobGen != 3 || recovered.OwnerRollGen != 2 {
		t.Fatalf("web fence not recovered: %+v", recovered)
	}
	if recovered.Plate != "p4" || recovered.PlateGeneration != 4 {
		t.Fatalf("plate not recovered: %+v", recovered)
	}
	if recovered.DryerProfile != "production" || recovered.DryerTarget != 95 || recovered.DryerGeneration != 3 {
		t.Fatalf("dryer not recovered: %+v", recovered)
	}
	if recovered.RegisterWindow == nil || recovered.RegisterWindow.PlateGeneration != 4 || recovered.RegisterWindow.Samples != 6 {
		t.Fatalf("register window not recovered: %+v", recovered.RegisterWindow)
	}
	if len(recovered.ReleasedRanges) != 1 {
		t.Fatalf("released interval not recovered: %+v", recovered.ReleasedRanges)
	}

	// Replaying the same release twice (duplicate event or a second replay of a
	// copied log) must not duplicate the released range.
	duplicated, err := Replay([]model.Event{jobEvent, fenceEvent, plateEvent, dryerEvent, windowEvent, releaseEvent, releaseEvent})
	if err != nil {
		t.Fatalf("replay duplicated: %v", err)
	}
	if len(duplicated.ReleasedRanges) != 1 {
		t.Fatalf("released range was duplicated by replay: %+v", duplicated.ReleasedRanges)
	}
}

func TestReplayRejectsFenceWithoutOwner(t *testing.T) {
	at := time.Now().UTC()
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 1, RollGeneration: 1, PlateGeneration: 1, CreatedAt: at, UpdatedAt: at}
	jobEvent := newTestEvent(t, "job_snapshot", job, 1, 1, at)
	emptyOwner := newTestEvent(t, "web_fence_restored", map[string]any{"owner": ""}, 1, 1, at.Add(time.Second))
	if _, err := Replay([]model.Event{jobEvent, emptyOwner}); err == nil {
		t.Fatal("expected a fence without an owner to be rejected")
	}
}

func TestReplayRejectsEventsWithoutJob(t *testing.T) {
	at := time.Now().UTC()
	// A valid fence event whose job was never snapshotted is corrupt evidence
	// (e.g. torn log), not a cold start.
	fence := newTestEvent(t, "web_fence_restored", map[string]any{"owner": "line-a"}, 1, 1, at)
	if _, err := Replay([]model.Event{fence}); err == nil {
		t.Fatal("expected a log with events but no job to be rejected")
	}
}
