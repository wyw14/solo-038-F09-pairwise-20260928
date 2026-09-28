package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
)

func testRuntime(t *testing.T) (*Runtime, string) {
	t.Helper()
	dataDir := t.TempDir()
	runtime, err := NewRuntime(Config{DataDir: dataDir, FestoonCapacity: 120, RegisterGap: 500 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return runtime, dataDir
}

func printingJob(now time.Time) model.Job {
	return model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 1, RollGeneration: 1, PlateGeneration: 2, CreatedAt: now, UpdatedAt: now}
}

func seedFullRun(t *testing.T, runtime *Runtime, jobValue model.Job, now time.Time) {
	t.Helper()
	must := func(sequence uint64, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seed event: %v", err)
		}
	}
	must(journal.PersistJobSnapshot(runtime.journal, jobValue, model.NewOperationID(), "job_snapshot", now))
	must(journal.PersistFence(runtime.journal, jobValue, model.NewOperationID(), string(jobValue.ID), now.Add(time.Second)))
	must(journal.PersistPlateChange(runtime.journal, jobValue, model.NewOperationID(), "plate-2", jobValue.PlateGeneration, now.Add(2*time.Second)))
	must(journal.PersistRegisterWindow(runtime.journal, jobValue, model.NewOperationID(), journal.RegisterWindowRecord{JobGeneration: jobValue.Generation, PlateGeneration: jobValue.PlateGeneration, StartMeter: 0, LastMeter: 10, Samples: 4}, now.Add(3*time.Second)))
	must(journal.PersistDryerStarted(runtime.journal, jobValue, model.NewOperationID(), 95, "production", now.Add(4*time.Second)))
	interval := model.Interval{JobID: jobValue.ID, JobGeneration: jobValue.Generation, RollGeneration: jobValue.RollGeneration, StartMeter: 0, EndMeter: 10, OpenedAt: now, ClosedAt: now.Add(5 * time.Second)}
	must(journal.PersistInterval(runtime.journal, jobValue, model.NewOperationID(), journal.IntervalRecord{Interval: interval, Released: true}, now.Add(5*time.Second)))
}

func matchingSnapshot(t *testing.T, dataDir string, jobValue model.Job) {
	t.Helper()
	values := map[string]any{
		"job":                    jobValue,
		"web_owner":              string(jobValue.ID),
		"web_job_generation":     jobValue.Generation,
		"web_roll_generation":    jobValue.RollGeneration,
		"web_threaded":           true,
		"web_meter":              10.5,
		"press_plate":            "plate-2",
		"press_plate_generation": jobValue.PlateGeneration,
		"press_color":            "cyan",
		"press_phases":           map[string]any{"cyan": 0.01},
		"register_window":        journal.RegisterWindowRecord{JobGeneration: 1, PlateGeneration: 2, StartMeter: 0, LastMeter: 10, Samples: 4},
		"dryer_job_id":           string(jobValue.ID),
		"dryer_generation":       jobValue.Generation,
		"dryer_target":           95.0,
		"dryer_profile":          "production",
		"dryer_temperature":      88.0,
		"dryer_active":           true,
	}
	if err := journal.SaveSnapshot(dataDir+"/runtime-snapshot.json", journal.Snapshot{Version: 7, Values: values}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
}

func item(report RestartReport, component string) ItemResult {
	for _, item := range report.Items {
		if item.Component == component {
			return item
		}
	}
	return ItemResult{Status: "missing"}
}

func TestRestartRecoveryRestoresConfirmedStateHoldingLineStopped(t *testing.T) {
	runtime, dataDir := testRuntime(t)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	jobValue := printingJob(now)
	seedFullRun(t, runtime, jobValue, now)
	matchingSnapshot(t, dataDir, jobValue)

	report, err := runtime.RestartRecover()
	if err != nil {
		t.Fatalf("restart recovery: %v", err)
	}
	if !report.RecoveredJob || !report.LineStopRequired {
		t.Fatalf("line must come back stopped: %+v", report)
	}
	for _, component := range []string{"job", "web", "unwind", "press", "register", "register_releases", "dryer"} {
		result := item(report, component)
		if result.Status == ItemManualReview {
			t.Fatalf("%s unexpectedly needs manual review: %s", component, result.Reason)
		}
		if component == "job" && result.Status != ItemRestored {
			t.Fatalf("job status = %q", result.Status)
		}
	}
	if result := item(report, "dryer"); result.Status != ItemStopped {
		t.Fatalf("dryer status = %q, want stopped", result.Status)
	}
	if result := item(report, "unwind"); result.Status != ItemStopped {
		t.Fatalf("unwind status = %q, want stopped", result.Status)
	}
	if result := item(report, "register"); result.Status != ItemStopped {
		t.Fatalf("register status = %q, want stopped", result.Status)
	}

	// Actuators must all be off even though their identity is restored.
	_, _, feedOwner, feedSpeed := runtime.unwind.Snapshot()
	if feedOwner == "" || feedSpeed != 0 {
		t.Fatalf("feed must be stopped, owner=%q speed=%v", feedOwner, feedSpeed)
	}
	_, _, _, _, _, dryerActive := runtime.dryer.Snapshot()
	if dryerActive {
		t.Fatal("dryer must not be heating after recovery")
	}
	if window := runtime.register.Snapshot(); window.Valid {
		t.Fatal("register window must be invalid after recovery")
	}
	current, ok := runtime.jobs.Current()
	if !ok || current.ID != jobValue.ID {
		t.Fatal("recovered job is not current")
	}
	if !runtime.registerControl.CanAdvance(current) {
		t.Fatal("released interval for the current generation should be restored")
	}
}

func TestRestartRecoveryIsIdempotent(t *testing.T) {
	runtime, dataDir := testRuntime(t)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	jobValue := printingJob(now)
	seedFullRun(t, runtime, jobValue, now)
	matchingSnapshot(t, dataDir, jobValue)

	first, err := runtime.RestartRecover()
	if err != nil {
		t.Fatalf("first recovery: %v", err)
	}
	second, err := runtime.RestartRecover()
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if second.EventCount != first.EventCount || second.UniqueEvents != first.UniqueEvents {
		t.Fatalf("replay counts drifted: %d/%d vs %d/%d", second.EventCount, second.UniqueEvents, first.EventCount, first.UniqueEvents)
	}
	statuses := map[string]string{}
	for _, result := range first.Items {
		statuses[result.Component] = result.Status
	}
	for _, result := range second.Items {
		if statuses[result.Component] != result.Status {
			t.Fatalf("item %s changed from %s to %s", result.Component, statuses[result.Component], result.Status)
		}
	}
	owner, jobGen, rollGen, _, meter := runtime.web.Snapshot()
	if owner != string(jobValue.ID) || jobGen != 1 || rollGen != 1 || meter != 10.5 {
		t.Fatalf("web state changed on second recovery: %s/%d/%d meter=%v", owner, jobGen, rollGen, meter)
	}
	// Journal sequences issued after restart must not collide with the
	// recovered high-water mark.
	sequence, err := runtime.journal.Append(newDryingEvent(t, jobValue))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if sequence <= uint64(first.UniqueEvents) {
		t.Fatalf("post-restart sequence %d collides with recovered mark %d", sequence, first.UniqueEvents)
	}
}

func TestRestartRecoveryFlagsStaleSnapshotGenerations(t *testing.T) {
	runtime, dataDir := testRuntime(t)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	jobValue := printingJob(now)
	seedFullRun(t, runtime, jobValue, now)

	// Snapshot from a newer roll/plate generation that the event log never
	// confirmed (e.g. change happened during the power failure window).
	stale := jobValue
	stale.RollGeneration = 2
	stale.PlateGeneration = 3
	matchingSnapshot(t, dataDir, stale)

	report, err := runtime.RestartRecover()
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !report.ManualReviewNeeded {
		t.Fatal("generation split must require manual review")
	}
	if result := item(report, "snapshot"); result.Status != ItemManualReview {
		t.Fatalf("snapshot status = %q, want manual_review", result.Status)
	}
	// Web fence is still confirmed from events, but snapshot detail (meter)
	// must not be trusted.
	if result := item(report, "web"); result.Status != ItemRestored || result.Detail["meter"] != 0.0 {
		t.Fatalf("web must be fenced from events with meter reset: %+v", result)
	}
	// The newer plate from the snapshot must never be installed.
	_, plateGen, _, _ := runtime.press.Snapshot()
	if plateGen != jobValue.PlateGeneration {
		t.Fatalf("unconfirmed plate generation %d installed", plateGen)
	}
	// Dryer snapshot active for gen 1 matches the event — it is still held
	// stopped, and the stale plate does not upgrade the job.
	current, _ := runtime.jobs.Current()
	if current.PlateGeneration != jobValue.PlateGeneration {
		t.Fatalf("job plate generation upgraded to %d without evidence", current.PlateGeneration)
	}
}

func TestRestartRecoveryHoldsLineWhenEvidenceIncomplete(t *testing.T) {
	runtime, _ := testRuntime(t)
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	jobValue := printingJob(now)
	// Only a job snapshot survives: no fence, no plate, no dryer, no window.
	if _, err := journal.PersistJobSnapshot(runtime.journal, jobValue, model.NewOperationID(), "job_snapshot", now); err != nil {
		t.Fatalf("seed: %v", err)
	}

	report, err := runtime.RestartRecover()
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !report.RecoveredJob || !report.LineStopRequired || !report.ManualReviewNeeded {
		t.Fatalf("incomplete evidence must hold the line for review: %+v", report)
	}
	if result := item(report, "web"); result.Status != ItemManualReview {
		t.Fatalf("web without a fence must need review, got %q", result.Status)
	}
	if result := item(report, "unwind"); result.Status != ItemManualReview {
		t.Fatalf("unwind without a fence must need review, got %q", result.Status)
	}
	if result := item(report, "press"); result.Status != ItemAbsent {
		t.Fatalf("press with no plate evidence should be absent, got %q", result.Status)
	}
	if result := item(report, "dryer"); result.Status != ItemAbsent {
		t.Fatalf("dryer with no start evidence should be absent, got %q", result.Status)
	}
	if _, _, owner, speed := runtime.unwind.Snapshot(); owner != "" || speed != 0 {
		t.Fatalf("unwind must remain untouched, owner=%q speed=%v", owner, speed)
	}
	if len(runtime.Incidents()) == 0 {
		t.Fatal("manual-review items must be recorded as incidents")
	}
}

func TestStartOnEmptyDataIsColdStart(t *testing.T) {
	runtime, _ := testRuntime(t)
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	current, ok := runtime.jobs.Current()
	if !ok {
		t.Fatal("cold start should create a job")
	}
	// Restarting the runtime over the now-populated log must recover that job
	// rather than create a second one.
	second, err := NewRuntime(Config{DataDir: runtime.config.DataDir, FestoonCapacity: 120, RegisterGap: 500 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatalf("second runtime: %v", err)
	}
	if err := second.Start(context.Background()); err != nil {
		t.Fatalf("second start: %v", err)
	}
	recovered, ok := second.jobs.Current()
	if !ok || recovered.ID != current.ID {
		t.Fatalf("restart did not recover the original job: got %v want %v", recovered.ID, current.ID)
	}
	if !second.recovered {
		t.Fatal("restart should mark the runtime as recovered")
	}
}

func newDryingEvent(t *testing.T, jobValue model.Job) model.Event {
	t.Helper()
	event, err := model.NewEvent(model.NewOperationID(), jobValue, "dryer_started", map[string]any{"target": 90.0, "profile": "production"}, time.Now())
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	return event
}
