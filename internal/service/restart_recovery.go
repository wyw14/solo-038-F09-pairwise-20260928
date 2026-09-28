package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/dryer"
	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
	"github.com/wyw14/solo-038/internal/register"
	"github.com/wyw14/solo-038/internal/unwind"
	"github.com/wyw14/solo-038/internal/web"
)

// Recovery item outcomes.
const (
	ItemRecovered = "recovered" // durable evidence confirmed the state at the current fence generation
	ItemStopped   = "stopped"   // state known, but motion/heat is deliberately held stopped pending manual go
	ItemReview    = "review"    // evidence missing or from another generation; operator must inspect
	ItemInactive  = "inactive"  // component was not engaged before the outage
)

// RecoveryItem is the per-component restart verdict.
type RecoveryItem struct {
	Component       string `json:"component"`
	Outcome         string `json:"outcome"`
	JobGeneration   uint64 `json:"job_generation"`
	EvidenceGen     uint64 `json:"evidence_generation"`
	RollGeneration  uint64 `json:"roll_generation,omitempty"`
	PlateGeneration uint64 `json:"plate_generation,omitempty"`
	Detail          string `json:"detail"`
	Action          string `json:"action"`
}

// RecoveryReport is the full real-restart verdict. Equipment is only released
// into automatic motion when every relevant item is recovered; otherwise the
// line is held stopped.
type RecoveryReport struct {
	Recovered     bool           `json:"recovered"`
	HeldStopped   bool           `json:"held_stopped"`
	NeedsReview   bool           `json:"needs_review"`
	JobID         model.JobID    `json:"job_id"`
	JobState      model.JobState `json:"job_state"`
	Generation    uint64         `json:"generation"`
	EventsRead    int            `json:"events_read"`
	EventsApplied int            `json:"events_applied"`
	Duplicates    int            `json:"duplicate_events"`
	Items         []RecoveryItem `json:"items"`
	RecoveredAt   time.Time      `json:"recovered_at"`
}

var recoveryReportMu sync.RWMutex

const recoveryIncidentPrefix = "restart-recovery: "

// RestartRecovery folds the durable journal (and the last snapshot) into
// runtime state. It restores only what durable evidence confirms at the
// current fence generation, holds all motion/heat stopped, and flags
// cross-generation components for manual review. Replaying the same events
// again produces the same report and state.
func (runtime *Runtime) RestartRecovery(events []model.Event) error {
	// A repeat replay must be a no-op on externally observable state, so drop
	// incidents raised by a prior recovery fold before re-applying.
	runtime.clearRecoveryIncidents()
	recovered, err := journal.Replay(events)
	if err != nil {
		return err
	}
	report := RecoveryReport{Recovered: recovered.HasJob, EventsRead: recovered.EventCount, EventsApplied: recovered.Replayed, Duplicates: recovered.Duplicates, RecoveredAt: time.Now().UTC()}

	if !recovered.HasJob {
		report.HeldStopped = true
		report.NeedsReview = true
		runtime.setRecoveryReport(report)
		return nil
	}

	jobValue := recovered.Job
	if err := runtime.jobs.Put(jobValue); err != nil {
		return err
	}
	report.JobID = jobValue.ID
	report.JobState = jobValue.State
	report.Generation = jobValue.Generation

	snapshot, snapshotErr := runtimeLoadSnapshot(runtime.config.DataDir)

	// --- Web / unwind feed -------------------------------------------------
	report.Items = append(report.Items, runtime.recoverWeb(jobValue, recovered, snapshot))

	// --- Press printing unit (plate generation) ----------------------------
	report.Items = append(report.Items, runtime.recoverPress(jobValue, recovered))

	// --- Register window ----------------------------------------------------
	report.Items = append(report.Items, runtime.recoverRegister(jobValue, recovered))

	// --- Dryer ---------------------------------------------------------------
	report.Items = append(report.Items, runtime.recoverDryer(jobValue, recovered))

	// Rebuild the released-interval ledger so already-released product is not
	// released twice.
	runtime.registerControl.Restore(recovered.Intervals)

	for _, item := range report.Items {
		if item.Outcome == ItemReview {
			report.NeedsReview = true
			runtime.RecordIncident("error", item.Component, recoveryIncidentPrefix+"manual review required: "+item.Detail)
		}
	}
	// After a power loss no component is permitted to move or heat on its own:
	// the line is held stopped until an operator resumes it, even when all
	// state was confirmed.
	report.HeldStopped = true
	if snapshotErr != nil && !errors.Is(snapshotErr, fs.ErrNotExist) {
		// A missing snapshot is normal (the journal is authoritative); only a
		// snapshot that exists but cannot be read is worth surfacing.
		runtime.RecordIncident("warning", "snapshot", recoveryIncidentPrefix+fmt.Sprintf("runtime snapshot unavailable: %v", snapshotErr))
	}

	runtime.setRecoveryReport(report)
	return nil
}

func (runtime *Runtime) recoverWeb(jobValue model.Job, recovered journal.RecoveredState, snapshot journal.Snapshot) RecoveryItem {
	item := RecoveryItem{Component: "web", JobGeneration: jobValue.Generation, RollGeneration: jobValue.RollGeneration}
	if !recovered.HasFence {
		item.Outcome = ItemReview
		item.Detail = "no durable web fence event; web owner and roll generation are unknown"
		item.Action = "inspect the web path and re-thread before restoring feed"
		return item
	}
	fence := recovered.Fence
	item.EvidenceGen = fence.JobGeneration
	if fence.JobGeneration != jobValue.Generation || fence.RollGeneration != jobValue.RollGeneration {
		item.Outcome = ItemReview
		item.Detail = fmt.Sprintf("web fence is job gen %d / roll gen %d but current job is gen %d / roll gen %d", fence.JobGeneration, fence.RollGeneration, jobValue.Generation, jobValue.RollGeneration)
		item.Action = "fence belongs to a different roll/job generation; re-thread and re-fence before feed"
		return item
	}
	threaded, meter := webSnapshotProgress(snapshot, fence.JobGeneration, fence.RollGeneration)
	record := web.Recovery{Owner: fence.Owner, JobGeneration: fence.JobGeneration, RollGeneration: fence.RollGeneration, Threaded: threaded, Meter: meter}
	if err := web.Restore(runtime.web, record); err != nil {
		item.Outcome = ItemReview
		item.Detail = "web fence could not be restored: " + err.Error()
		item.Action = "inspect the web path manually"
		return item
	}
	// Feed is never auto-resumed: restore identity only, keep speed at zero.
	unwindRecord := unwind.RecoveryRecord{Roll: "recovered-roll", RollGeneration: fence.RollGeneration, Owner: fence.Owner, Speed: 0}
	if err := unwind.Restore(runtime.unwind, unwindRecord); err != nil {
		item.Outcome = ItemReview
		item.Detail = "unwind feed fence could not be restored: " + err.Error()
		item.Action = "inspect unwind feed manually"
		return item
	}
	item.Outcome = ItemStopped
	item.Detail = fmt.Sprintf("web fence restored (owner %s, roll gen %d, threaded=%t, meter=%.2f); feed held stopped", fence.Owner, fence.RollGeneration, threaded, meter)
	item.Action = "confirm the web path and press resume-feed to restart at the safe crawl speed"
	return item
}

func (runtime *Runtime) recoverPress(jobValue model.Job, recovered journal.RecoveredState) RecoveryItem {
	item := RecoveryItem{Component: "press", JobGeneration: jobValue.Generation, PlateGeneration: jobValue.PlateGeneration}
	if !recovered.HasPlate {
		if jobValue.State == model.JobLoading || jobValue.State == model.JobThreading {
			item.Outcome = ItemInactive
			item.Detail = "no plate evidence and job had not reached the press"
			item.Action = "none"
			return item
		}
		item.Outcome = ItemReview
		item.Detail = "no durable plate_changed event; mounted plate generation is unknown"
		item.Action = "verify the mounted plate and re-register before printing"
		return item
	}
	plate := recovered.Plate
	item.EvidenceGen = plate.PlateGeneration
	if plate.JobGeneration != jobValue.Generation || plate.PlateGeneration != jobValue.PlateGeneration {
		item.Outcome = ItemReview
		item.Detail = fmt.Sprintf("plate evidence gen %d (job gen %d) does not match current plate gen %d (job gen %d)", plate.PlateGeneration, plate.JobGeneration, jobValue.PlateGeneration, jobValue.Generation)
		item.Action = "the mounted plate is from another generation; confirm or remount before printing"
		return item
	}
	// Restore the plate identity only. Phase corrections are deliberately not
	// replayed: register must be re-confirmed with fresh frames.
	if err := press.RestorePhases(runtime.press, plate.Plate, plate.PlateGeneration, map[string]float64{}); err != nil {
		item.Outcome = ItemReview
		item.Detail = "plate could not be restored: " + err.Error()
		item.Action = "inspect the press manually"
		return item
	}
	item.Outcome = ItemRecovered
	item.Detail = fmt.Sprintf("plate %q restored at generation %d; unit speeds held at zero", plate.Plate, plate.PlateGeneration)
	item.Action = "re-run register adjustment with fresh camera frames before impression"
	return item
}

func (runtime *Runtime) recoverRegister(jobValue model.Job, recovered journal.RecoveredState) RecoveryItem {
	item := RecoveryItem{Component: "register", JobGeneration: jobValue.Generation, PlateGeneration: jobValue.PlateGeneration}
	if !recovered.HasRegister {
		item.Outcome = ItemInactive
		item.Detail = "no durable register window was open before the outage"
		item.Action = "open a fresh register window when the press is running"
		return item
	}
	evidence := recovered.Register
	item.EvidenceGen = evidence.PlateGeneration
	if evidence.JobGeneration != jobValue.Generation || evidence.PlateGeneration != jobValue.PlateGeneration {
		item.Outcome = ItemReview
		item.Detail = fmt.Sprintf("register window plate gen %d (job gen %d) does not match current plate gen %d (job gen %d)", evidence.PlateGeneration, evidence.JobGeneration, jobValue.PlateGeneration, jobValue.Generation)
		item.Action = "discard the stale window and open a new one for the mounted plate"
		return item
	}
	record := register.Recovery{JobGeneration: evidence.JobGeneration, PlateGeneration: evidence.PlateGeneration, StartMeter: evidence.StartMeter, StartedAt: evidence.StartedAt}
	if err := register.Restore(runtime.register, record); err != nil {
		item.Outcome = ItemReview
		item.Detail = "register window could not be restored: " + err.Error()
		item.Action = "inspect the register system manually"
		return item
	}
	item.Outcome = ItemRecovered
	item.Detail = fmt.Sprintf("register window reopened for plate gen %d from meter %.2f with zero trusted samples", evidence.PlateGeneration, evidence.StartMeter)
	item.Action = "collect fresh camera frames before releasing the next interval"
	return item
}

func (runtime *Runtime) recoverDryer(jobValue model.Job, recovered journal.RecoveredState) RecoveryItem {
	item := RecoveryItem{Component: "dryer", JobGeneration: jobValue.Generation}
	if !recovered.HasDryer {
		item.Outcome = ItemInactive
		item.Detail = "no durable dryer heat request before the outage"
		item.Action = "start the dryer profile when production resumes"
		return item
	}
	evidence := recovered.Dryer
	item.EvidenceGen = evidence.Generation
	if evidence.JobID != jobValue.ID || evidence.Generation != jobValue.Generation {
		item.Outcome = ItemReview
		item.Detail = fmt.Sprintf("dryer request gen %d (job %s) does not match current job gen %d (%s)", evidence.Generation, evidence.JobID, jobValue.Generation, jobValue.ID)
		item.Action = "dryer request belongs to another job; confirm before applying heat"
		return item
	}
	// Restore the requested target/profile, but never the heat itself.
	record := dryer.Recovery{JobID: string(evidence.JobID), Generation: evidence.Generation, Target: evidence.Target, Profile: evidence.Profile}
	if err := dryer.Restore(runtime.dryer, record); err != nil {
		item.Outcome = ItemReview
		item.Detail = "dryer request could not be restored: " + err.Error()
		item.Action = "inspect the dryer manually"
		return item
	}
	item.Outcome = ItemStopped
	item.Detail = fmt.Sprintf("dryer request restored (profile %q, target %.0f) but heater is cold/inactive", evidence.Profile, evidence.Target)
	item.Action = "re-arm the dryer ramp manually and wait for ready before web release"
	return item
}

// webSnapshotProgress extracts threaded/meter from the snapshot only when the
// snapshot fence generations match the durable fence. A stale or torn
// snapshot contributes nothing.
func webSnapshotProgress(snapshot journal.Snapshot, jobGeneration, rollGeneration uint64) (bool, float64) {
	if snapshot.Values == nil {
		return false, 0
	}
	if snapshotUint(snapshot, "web_job_generation") != jobGeneration || snapshotUint(snapshot, "web_roll_generation") != rollGeneration {
		return false, 0
	}
	threaded, _ := snapshot.Values["web_threaded"].(bool)
	meter := snapshotFloat(snapshot, "web_meter")
	return threaded, meter
}

func snapshotUint(snapshot journal.Snapshot, key string) uint64 {
	switch value := snapshot.Values[key].(type) {
	case float64:
		return uint64(value)
	case json.Number:
		n, _ := value.Int64()
		return uint64(n)
	default:
		return 0
	}
}

func snapshotFloat(snapshot journal.Snapshot, key string) float64 {
	switch value := snapshot.Values[key].(type) {
	case float64:
		return value
	case json.Number:
		f, _ := value.Float64()
		return f
	default:
		return 0
	}
}

func (runtime *Runtime) setRecoveryReport(report RecoveryReport) {
	recoveryReportMu.Lock()
	defer recoveryReportMu.Unlock()
	runtime.recoveryReport = &report
}

// RecoveryReport returns the last restart recovery verdict.
func (runtime *Runtime) RecoveryReport() (RecoveryReport, bool) {
	recoveryReportMu.RLock()
	defer recoveryReportMu.RUnlock()
	if runtime.recoveryReport == nil {
		return RecoveryReport{}, false
	}
	return *runtime.recoveryReport, true
}
