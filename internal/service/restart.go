package service

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/press"
	"github.com/wyw14/solo-038/internal/register"
	"github.com/wyw14/solo-038/internal/unwind"
	"github.com/wyw14/solo-038/internal/web"
)

// Item outcome of restart recovery.
const (
	// ItemRestored: durable evidence confirmed the item and its state was installed.
	ItemRestored = "restored"
	// ItemStopped: identity was restored but the actuator is held stopped and an
	// operator must deliberately re-enable motion/heat.
	ItemStopped = "stopped"
	// ItemManualReview: evidence is missing or split across generations; nothing
	// was installed and the line stays down until a human checks it.
	ItemManualReview = "manual_review"
	// ItemAbsent: no durable evidence the item was ever in service.
	ItemAbsent = "absent"
)

// ItemResult is the per-component recovery verdict an operator sees after a
// power-loss restart.
type ItemResult struct {
	Component       string         `json:"component"`
	Status          string         `json:"status"`
	Reason          string         `json:"reason"`
	JobGeneration   uint64         `json:"job_generation,omitempty"`
	RollGeneration  uint64         `json:"roll_generation,omitempty"`
	PlateGeneration uint64         `json:"plate_generation,omitempty"`
	Detail          map[string]any `json:"detail,omitempty"`
}

// RestartReport is the complete restart recovery verdict.
type RestartReport struct {
	RecoveredAt        time.Time    `json:"recovered_at"`
	RecoveredJob       bool         `json:"recovered_job"`
	LineStopRequired   bool         `json:"line_stop_required"`
	ManualReviewNeeded bool         `json:"manual_review_needed"`
	SnapshotVersion    uint64       `json:"snapshots_version,omitempty"`
	EventCount         int          `json:"event_count"`
	UniqueEvents       int          `json:"unique_events"`
	DuplicatesSkipped  int          `json:"duplicates_skipped"`
	Items              []ItemResult `json:"items"`
	Notes              []string     `json:"notes"`
	Job                *model.Job   `json:"job,omitempty"`
}

func (report *RestartReport) add(item ItemResult) {
	report.Items = append(report.Items, item)
	if item.Status == ItemManualReview {
		report.ManualReviewNeeded = true
	}
}

func (report *RestartReport) note(message string) {
	report.Notes = append(report.Notes, message)
}

// RestartRecover rebuilds runtime state after a power loss from the durable
// event log, using the atomic snapshot only for detail state whose fence
// generations match the log. It never starts feed, press motion, register
// release, or heat: anything it cannot confirm by fence generation is left
// stopped and flagged for manual review. Running it repeatedly converges to
// the same report and the same component state.
func (runtime *Runtime) RestartRecover() (RestartReport, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.restartRecoverLocked()
}

// RestartReport returns the verdict of the most recent restart recovery.
func (runtime *Runtime) RestartReport() (RestartReport, bool) {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if runtime.restartReport == nil {
		return RestartReport{}, false
	}
	return *runtime.restartReport, true
}

func (runtime *Runtime) restartRecoverLocked() (RestartReport, error) {
	report := RestartReport{RecoveredAt: time.Now().UTC(), Items: []ItemResult{}}

	events, err := runtime.journal.Events()
	if err != nil {
		return report, fmt.Errorf("read durable event log: %w", err)
	}
	recovered, err := journal.Replay(events)
	if err != nil {
		return report, fmt.Errorf("replay durable event log: %w", err)
	}
	report.EventCount = recovered.EventCount
	report.UniqueEvents = recovered.UniqueEventCount
	report.DuplicatesSkipped = recovered.DuplicatesSkipped
	runtime.journal.State().Seed(uint64(recovered.UniqueEventCount))

	snapshot, snapshotErr := runtimeLoadSnapshot(runtime.config.DataDir)
	hasSnapshot := !errors.Is(snapshotErr, os.ErrNotExist)
	if snapshotErr != nil && hasSnapshot {
		return report, fmt.Errorf("load runtime snapshot: %w", snapshotErr)
	}
	var snap RuntimeSnapshot
	if hasSnapshot {
		snap, err = decodeRuntimeSnapshot(snapshot)
		if err != nil {
			return report, err
		}
		report.SnapshotVersion = snap.Version
	}

	haveJob := recovered.Job.ID.Valid()
	switch {
	case haveJob:
		jobValue := recovered.Job
		if err := runtime.jobs.Put(jobValue); err != nil {
			return report, fmt.Errorf("restore job: %w", err)
		}
		report.RecoveredJob = true
		report.Job = &jobValue
		report.add(ItemResult{
			Component: "job", Status: ItemRestored,
			Reason:        "job rebuilt from durable events",
			JobGeneration: jobValue.Generation, RollGeneration: jobValue.RollGeneration, PlateGeneration: jobValue.PlateGeneration,
			Detail: map[string]any{"state": string(jobValue.State)},
		})
		if jobValue.State == model.JobCancelled || jobValue.State == model.JobComplete {
			report.note(fmt.Sprintf("recovered job is %s; the line remains stopped", jobValue.State))
		}
	case hasSnapshot && snap.Job.ID.Valid():
		// Snapshot without a single durable event is exactly the "power died
		// mid-write" split-brain case: nothing is confirmed.
		report.add(ItemResult{Component: "job", Status: ItemManualReview, Reason: "snapshot exists but no durable job event confirms it"})
	default:
		report.note("no durable job evidence; this is a cold start")
	}

	snapshotCurrent := hasSnapshot && haveJob &&
		snap.Job.ID == recovered.Job.ID &&
		snap.Job.Generation == recovered.Job.Generation &&
		snap.Job.RollGeneration == recovered.Job.RollGeneration
	if hasSnapshot && haveJob && !snapshotCurrent {
		report.add(ItemResult{Component: "snapshot", Status: ItemManualReview, Reason: "snapshot fence generations do not match the durable job", Detail: map[string]any{
			"snapshot_job":             string(snap.Job.ID),
			"snapshot_generation":      snap.Job.Generation,
			"snapshot_roll_generation": snap.Job.RollGeneration,
			"event_job":                string(recovered.Job.ID),
			"event_generation":         recovered.Job.Generation,
			"event_roll_generation":    recovered.Job.RollGeneration,
		}})
		report.note("snapshot detail is untrusted; only event-proven state is restored")
	}
	if !hasSnapshot && haveJob {
		report.note("no runtime snapshot; recovering identity from events only, detail values start safe/zero")
	}

	if haveJob {
		runtime.recoverWeb(recovered, snap, snapshotCurrent, &report)
		runtime.recoverUnwind(recovered, &report)
		runtime.recoverPress(recovered, snap, snapshotCurrent, &report)
		runtime.recoverRegister(recovered, snap, snapshotCurrent, &report)
		runtime.recoverDryer(recovered, snap, snapshotCurrent, &report)
	} else {
		for _, component := range []string{"web", "unwind", "press", "register", "dryer"} {
			report.add(ItemResult{Component: component, Status: ItemAbsent, Reason: "no recovered job to fence the component"})
		}
	}

	// A recovered line is never allowed to run away from a reboot: feed, press
	// units and heaters all came back stopped, so an operator restart is always
	// required to move the line.
	if report.RecoveredJob {
		report.LineStopRequired = true
	}

	runtime.restartReport = &report
	runtime.recovered = report.RecoveredJob
	for _, item := range report.Items {
		if item.Status == ItemManualReview {
			runtime.RecordIncident("error", item.Component, "restart recovery needs manual review: "+item.Reason)
		}
	}
	return report, nil
}

func (runtime *Runtime) recoverWeb(recovered journal.RecoveredState, snap RuntimeSnapshot, snapshotCurrent bool, report *RestartReport) {
	jobValue := recovered.Job
	switch {
	case recovered.Owner == "":
		report.add(ItemResult{Component: "web", Status: ItemManualReview, Reason: "no durable web fence event; web ownership cannot be confirmed after power loss", JobGeneration: jobValue.Generation, RollGeneration: jobValue.RollGeneration})
		return
	case recovered.OwnerJobGen != jobValue.Generation || recovered.OwnerRollGen != jobValue.RollGeneration:
		report.add(ItemResult{Component: "web", Status: ItemManualReview, Reason: "web fence belongs to a different generation than the recovered job", JobGeneration: jobValue.Generation, RollGeneration: jobValue.RollGeneration, Detail: map[string]any{
			"fence_job_generation":  recovered.OwnerJobGen,
			"fence_roll_generation": recovered.OwnerRollGen,
		}})
		return
	}

	threaded, meter := false, 0.0
	snapshotMatched := snapshotCurrent && snap.WebOwner == recovered.Owner &&
		snap.WebJobGeneration == recovered.OwnerJobGen &&
		snap.WebRollGeneration == recovered.OwnerRollGen
	if snapshotMatched {
		threaded = snap.WebThreaded
		meter = snap.WebMeter
	} else if snapshotCurrent {
		report.note("web snapshot fence disagrees with the durable fence event; meter reset to zero")
	}
	record := web.Recovery{Owner: recovered.Owner, JobGeneration: recovered.OwnerJobGen, RollGeneration: recovered.OwnerRollGen, Threaded: threaded, Meter: meter}
	if err := web.Restore(runtime.web, record); err != nil {
		report.add(ItemResult{Component: "web", Status: ItemManualReview, Reason: "web fence restore rejected: " + err.Error()})
		return
	}
	report.add(ItemResult{Component: "web", Status: ItemRestored, Reason: "web fence restored from durable event; drive remains stopped", JobGeneration: recovered.OwnerJobGen, RollGeneration: recovered.OwnerRollGen, Detail: map[string]any{
		"owner": recovered.Owner, "threaded": threaded, "meter": meter, "drive": "stopped",
	}})
}

func (runtime *Runtime) recoverUnwind(recovered journal.RecoveredState, report *RestartReport) {
	if recovered.Owner == "" || recovered.OwnerJobGen != recovered.Job.Generation || recovered.OwnerRollGen != recovered.Job.RollGeneration {
		report.add(ItemResult{Component: "unwind", Status: ItemManualReview, Reason: "unwind feed owner is not durably fenced for the recovered generation"})
		return
	}
	// Speed is deliberately restored at zero: feed never auto-restarts.
	record := unwind.RecoveryRecord{Roll: "recovered-roll", RollGeneration: recovered.OwnerRollGen, Owner: recovered.Owner, Speed: 0}
	if err := unwind.Restore(runtime.unwind, record); err != nil {
		report.add(ItemResult{Component: "unwind", Status: ItemManualReview, Reason: "unwind restore rejected: " + err.Error()})
		return
	}
	report.add(ItemResult{Component: "unwind", Status: ItemStopped, Reason: "roll generation and feed owner restored; physical roll identity must be confirmed and feed restarted by an operator", RollGeneration: recovered.OwnerRollGen, Detail: map[string]any{
		"owner": recovered.Owner, "feed_speed": 0.0, "roll_id_confirmed": false,
	}})
}

func (runtime *Runtime) recoverPress(recovered journal.RecoveredState, snap RuntimeSnapshot, snapshotCurrent bool, report *RestartReport) {
	jobValue := recovered.Job
	switch {
	case recovered.PlateGeneration == 0:
		if snapshotCurrent && snap.PressPlateGeneration != 0 {
			report.add(ItemResult{Component: "press", Status: ItemManualReview, Reason: "snapshot shows a plate but no durable plate_changed event confirms it", PlateGeneration: snap.PressPlateGeneration})
			return
		}
		report.add(ItemResult{Component: "press", Status: ItemAbsent, Reason: "no plate change recorded before the power loss"})
		return
	case jobValue.PlateGeneration != 0 && recovered.PlateGeneration != jobValue.PlateGeneration:
		report.add(ItemResult{Component: "press", Status: ItemManualReview, Reason: "durable plate generation does not match the recovered job plate generation", JobGeneration: jobValue.Generation, PlateGeneration: recovered.PlateGeneration, Detail: map[string]any{
			"job_plate_generation": jobValue.PlateGeneration,
		}})
		return
	}

	phasesConfirmed := false
	var phases map[string]float64
	color := ""
	if snapshotCurrent && snap.PressPlateGeneration == recovered.PlateGeneration && snap.PressPlate == recovered.Plate {
		phases = snap.PressPhases
		color = snap.PressColor
		phasesConfirmed = true
	} else if snapshotCurrent {
		report.note("press snapshot belongs to another plate generation; phase table discarded")
	}
	if err := press.Restore(runtime.press, recovered.Plate, recovered.PlateGeneration, color, phases); err != nil {
		report.add(ItemResult{Component: "press", Status: ItemManualReview, Reason: "press restore rejected: " + err.Error(), PlateGeneration: recovered.PlateGeneration})
		return
	}
	item := ItemResult{Component: "press", Status: ItemRestored, Reason: "plate generation restored from durable event; all print units are stopped", JobGeneration: jobValue.Generation, PlateGeneration: recovered.PlateGeneration, Detail: map[string]any{
		"plate":            recovered.Plate,
		"phases_confirmed": phasesConfirmed,
		"units":            "stopped",
	}}
	if !phasesConfirmed {
		item.Status = ItemStopped
		item.Reason = "plate identity confirmed but the phase table is unconfirmed; re-register before printing"
	}
	report.add(item)
}

func (runtime *Runtime) recoverRegister(recovered journal.RecoveredState, snap RuntimeSnapshot, snapshotCurrent bool, report *RestartReport) {
	jobValue := recovered.Job
	if recovered.RegisterWindow == nil {
		report.add(ItemResult{Component: "register", Status: ItemAbsent, Reason: "no register window was open in the durable log"})
	} else {
		windowRec := *recovered.RegisterWindow
		if windowRec.JobGeneration != jobValue.Generation {
			report.add(ItemResult{Component: "register", Status: ItemManualReview, Reason: "register window belongs to another job generation", JobGeneration: windowRec.JobGeneration, PlateGeneration: windowRec.PlateGeneration})
		} else {
			samplesConfirmed := false
			if snapshotCurrent && snap.RegisterWindow != nil &&
				snap.RegisterWindow.JobGeneration == windowRec.JobGeneration &&
				snap.RegisterWindow.PlateGeneration == windowRec.PlateGeneration {
				windowRec.StartMeter = snap.RegisterWindow.StartMeter
				windowRec.LastMeter = snap.RegisterWindow.LastMeter
				windowRec.Samples = snap.RegisterWindow.Samples
				samplesConfirmed = true
			}
			window := register.Window{JobGeneration: windowRec.JobGeneration, PlateGeneration: windowRec.PlateGeneration, StartMeter: windowRec.StartMeter, LastMeter: windowRec.LastMeter, Samples: windowRec.Samples}
			if err := runtime.register.Restore(window); err != nil {
				report.add(ItemResult{Component: "register", Status: ItemManualReview, Reason: "register restore rejected: " + err.Error()})
			} else {
				report.add(ItemResult{Component: "register", Status: ItemStopped, Reason: "register window recovered but invalidated; camera dwell must be re-acquired before any release", JobGeneration: windowRec.JobGeneration, PlateGeneration: windowRec.PlateGeneration, Detail: map[string]any{
					"samples_confirmed": samplesConfirmed,
					"samples":           windowRec.Samples,
					"window_valid":      false,
				}})
			}
		}
	}

	// Restore only releases whose fence generations match the recovered job;
	// releases for older rolls/plates stay out of the ledger.
	current, stale := 0, 0
	for _, interval := range recovered.ReleasedRanges {
		if interval.JobID == jobValue.ID && interval.JobGeneration == jobValue.Generation && interval.RollGeneration == jobValue.RollGeneration {
			current++
		} else {
			stale++
		}
	}
	runtime.registerControl.RestoreReleased(jobValue, recovered.ReleasedRanges)
	if current > 0 || stale > 0 {
		item := ItemResult{Component: "register_releases", Status: ItemRestored, Reason: "durable released intervals rebuilt for the current generation", Detail: map[string]any{"restored": current, "stale_ignored": stale}}
		if stale > 0 {
			item.Status = ItemStopped
			item.Reason = "some released intervals belong to older generations and were not trusted; review before advancing"
		}
		report.add(item)
	} else {
		report.add(ItemResult{Component: "register_releases", Status: ItemAbsent, Reason: "no durably released intervals"})
	}
}

func (runtime *Runtime) recoverDryer(recovered journal.RecoveredState, snap RuntimeSnapshot, snapshotCurrent bool, report *RestartReport) {
	jobValue := recovered.Job
	switch {
	case recovered.DryerGeneration == 0:
		if snapshotCurrent && snap.DryerActive {
			report.add(ItemResult{Component: "dryer", Status: ItemManualReview, Reason: "snapshot shows an active dryer but no durable dryer_started event confirms it"})
			return
		}
		report.add(ItemResult{Component: "dryer", Status: ItemAbsent, Reason: "no dryer start recorded before the power loss"})
		return
	case recovered.DryerJobID != string(jobValue.ID) || recovered.DryerGeneration != jobValue.Generation:
		report.add(ItemResult{Component: "dryer", Status: ItemManualReview, Reason: "dryer was started under a different job generation", JobGeneration: recovered.DryerGeneration})
		return
	}

	if snapshotCurrent && snap.DryerActive && snap.DryerGeneration != recovered.DryerGeneration {
		report.add(ItemResult{Component: "dryer", Status: ItemManualReview, Reason: "dryer snapshot is active for a different generation than the durable start"})
		return
	}
	// The heater always comes back cold and inactive; the operator re-ramps.
	if err := runtime.dryer.Restore(recovered.DryerJobID, recovered.DryerGeneration, recovered.DryerTarget, recovered.DryerProfile); err != nil {
		report.add(ItemResult{Component: "dryer", Status: ItemManualReview, Reason: "dryer restore rejected: " + err.Error(), JobGeneration: recovered.DryerGeneration})
		return
	}
	report.add(ItemResult{Component: "dryer", Status: ItemStopped, Reason: "dryer job/profile restored from durable event; heater is held off pending an operator re-ramp", JobGeneration: recovered.DryerGeneration, Detail: map[string]any{
		"profile":     recovered.DryerProfile,
		"target":      recovered.DryerTarget,
		"temperature": 0.0,
		"heating":     false,
	}})
}
