package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/register"
)

func testRuntime(t *testing.T, dataDir string) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: dataDir, FestoonCapacity: 120, RegisterGap: 500 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return runtime
}

func itemByComponent(report RecoveryReport) map[string]RecoveryItem {
	items := map[string]RecoveryItem{}
	for _, item := range report.Items {
		items[item.Component] = item
	}
	return items
}

func recoveredPrintingJob() model.Job {
	now := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	job := model.NewJob(now)
	job.State = model.JobPrinting
	job.RollGeneration = 2
	job.PlateGeneration = 3
	return job
}

// appendRaw writes a hand-built event so payload/envelope generations can be
// deliberately mismatched in tests.
func appendRaw(t *testing.T, store *journal.Store, job model.Job, kind string, payload any, at time.Time) {
	t.Helper()
	event, err := model.NewEvent(model.NewOperationID(), job, kind, payload, at)
	if err != nil {
		t.Fatalf("event %s: %v", kind, err)
	}
	if _, err := store.Append(event); err != nil {
		t.Fatalf("append %s: %v", kind, err)
	}
}

func writeConfirmedHistory(t *testing.T, store *journal.Store, job model.Job) {
	t.Helper()
	base := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	if _, err := journal.PersistJobSnapshot(store, job, model.NewOperationID(), base); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PersistFence(store, job, model.NewOperationID(), string(job.ID), base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PersistPlate(store, job, model.NewOperationID(), journal.PlateRecord{Plate: "plate-A", PlateGeneration: job.PlateGeneration}, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PersistRegisterWindow(store, job, model.NewOperationID(), journal.RegisterWindowRecord{JobGeneration: job.Generation, PlateGeneration: job.PlateGeneration, StartMeter: 12}, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PersistDryerStart(store, job, model.NewOperationID(), journal.DryerStartRecord{Generation: job.Generation, Target: 100, Profile: "production"}, base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestRestartRecoveryConfirmedStateHeldStopped(t *testing.T) {
	dir := t.TempDir()
	runtime := testRuntime(t, dir)
	job := recoveredPrintingJob()
	writeConfirmedHistory(t, runtime.journal, job)

	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	report, ok := runtime.RecoveryReport()
	if !ok {
		t.Fatal("expected a recovery report after restart with history")
	}
	if !report.Recovered || !report.HeldStopped || report.NeedsReview {
		t.Fatalf("unexpected report flags: %+v", report)
	}
	items := itemByComponent(report)

	if items["web"].Outcome != ItemStopped {
		t.Fatalf("web should be held stopped, got %s (%s)", items["web"].Outcome, items["web"].Detail)
	}
	if items["press"].Outcome != ItemRecovered {
		t.Fatalf("press should be recovered, got %s (%s)", items["press"].Outcome, items["press"].Detail)
	}
	if items["register"].Outcome != ItemRecovered {
		t.Fatalf("register should be recovered, got %s (%s)", items["register"].Outcome, items["register"].Detail)
	}
	if items["dryer"].Outcome != ItemStopped {
		t.Fatalf("dryer should be held stopped, got %s (%s)", items["dryer"].Outcome, items["dryer"].Detail)
	}

	// Feed must be at zero even though the fence is confirmed.
	if _, _, _, speed := runtime.unwind.Snapshot(); speed != 0 {
		t.Fatalf("feed speed must be zero after restart, got %v", speed)
	}
	// Dryer must be cold/inactive even though a heat request existed.
	if _, _, _, _, _, active := runtime.dryer.Snapshot(); active {
		t.Fatal("dryer must be inactive after restart")
	}
	// Press plate identity restored, register window valid with zero samples.
	_, plateGen, _, _ := runtime.press.Snapshot()
	if plateGen != job.PlateGeneration {
		t.Fatalf("plate generation not restored: %d", plateGen)
	}
	window := runtime.register.Snapshot()
	if !window.Valid || window.Samples != 0 || window.PlateGeneration != job.PlateGeneration || window.StartMeter != 12 {
		t.Fatalf("register window not restored fenced/empty: %+v", window)
	}
}

func TestRestartRecoveryMixedGenerationsNeedReview(t *testing.T) {
	dir := t.TempDir()
	runtime := testRuntime(t, dir)
	job := recoveredPrintingJob() // roll gen 2, plate gen 3
	base := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)

	if _, err := journal.PersistJobSnapshot(runtime.journal, job, model.NewOperationID(), base); err != nil {
		t.Fatal(err)
	}
	// Fence from an older roll generation.
	staleRoll := job
	staleRoll.RollGeneration = 1
	appendRaw(t, runtime.journal, staleRoll, "web_fence_restored", map[string]any{"owner": string(job.ID), "roll_generation": float64(1)}, base.Add(time.Second))
	// Plate evidence from an older plate generation.
	appendRaw(t, runtime.journal, job, "plate_changed", journal.PlateRecord{Plate: "plate-old", PlateGeneration: 2}, base.Add(2*time.Second))
	// Register window matches the current plate; dryer matches current job.
	if _, err := journal.PersistRegisterWindow(runtime.journal, job, model.NewOperationID(), journal.RegisterWindowRecord{JobGeneration: 1, PlateGeneration: 3, StartMeter: 4}, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PersistDryerStart(runtime.journal, job, model.NewOperationID(), journal.DryerStartRecord{Generation: 1, Target: 90, Profile: "production"}, base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	report, _ := runtime.RecoveryReport()
	if !report.HeldStopped || !report.NeedsReview {
		t.Fatalf("mixed generations must hold the line and require review: %+v", report)
	}
	items := itemByComponent(report)
	if items["web"].Outcome != ItemReview || items["press"].Outcome != ItemReview {
		t.Fatalf("web and press from foreign generations must need review: %+v %+v", items["web"], items["press"])
	}
	if items["register"].Outcome != ItemRecovered || items["dryer"].Outcome != ItemStopped {
		t.Fatalf("matched register/dryer should restore: %+v %+v", items["register"], items["dryer"])
	}

	// The stale fence must not have been applied to web state.
	if owner, _, _, _, _ := runtime.web.Snapshot(); owner != "" {
		t.Fatalf("cross-generation web fence must not be restored, owner=%q", owner)
	}
	if _, _, _, _, _, active := runtime.dryer.Snapshot(); active {
		t.Fatal("dryer must remain inactive")
	}
	if len(runtime.Incidents()) == 0 {
		t.Fatal("review items must raise incidents")
	}
}

func TestRestartRecoveryReplayIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	runtime := testRuntime(t, dir)
	job := recoveredPrintingJob()
	writeConfirmedHistory(t, runtime.journal, job)

	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	events, err := runtime.journal.Events()
	if err != nil {
		t.Fatal(err)
	}
	// Introduce a cross-generation fence so at least one item needs review,
	// then replay the same events repeatedly.
	staleRoll := job
	staleRoll.RollGeneration = 1
	appendRaw(t, runtime.journal, staleRoll, "web_fence_restored", map[string]any{"owner": string(job.ID), "roll_generation": float64(1)}, time.Now())
	events, _ = runtime.journal.Events()

	if err := runtime.RestartRecovery(events); err != nil {
		t.Fatal(err)
	}
	afterSecond := len(runtime.Incidents())
	windowAfterSecond := runtime.register.Snapshot()
	reportAfterSecond, _ := runtime.RecoveryReport()

	if err := runtime.RestartRecovery(events); err != nil {
		t.Fatal(err)
	}
	afterThird := len(runtime.Incidents())
	windowAfterThird := runtime.register.Snapshot()
	reportAfterThird, _ := runtime.RecoveryReport()

	if afterSecond != afterThird {
		t.Fatalf("repeated replay accumulated incidents: %d vs %d", afterSecond, afterThird)
	}
	if windowAfterSecond != windowAfterThird {
		t.Fatalf("repeated replay changed register window: %+v vs %+v", windowAfterSecond, windowAfterThird)
	}
	if reportAfterSecond.NeedsReview != reportAfterThird.NeedsReview ||
		len(reportAfterSecond.Items) != len(reportAfterThird.Items) {
		t.Fatalf("repeated replay changed the recovery report")
	}
}

func TestColdStartWithoutHistoryCreatesFreshJob(t *testing.T) {
	dir := t.TempDir()
	runtime := testRuntime(t, dir)
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, ok := runtime.CurrentJob()
	if !ok || current.State != model.JobLoading {
		t.Fatalf("expected a fresh loading job, got %+v", current)
	}
	if _, ok := runtime.RecoveryReport(); ok {
		t.Fatal("cold start should not produce a recovery report")
	}
	// Ensure the new helpers' types are referenced (avoids accidental drift).
	_ = register.Recovery{}
	_ = dryer.Recovery{}
}
