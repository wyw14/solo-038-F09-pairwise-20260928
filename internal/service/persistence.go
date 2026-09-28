package service

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
)

// RuntimeSnapshot is the typed view of the last durable runtime snapshot.
// Every component carries the fence generations it was captured under, so a
// restart can only trust a component snapshot when those generations match
// the fence recovered from the event log.
type RuntimeSnapshot struct {
	Version uint64
	Job     model.Job

	WebOwner          string
	WebJobGeneration  uint64
	WebRollGeneration uint64
	WebThreaded       bool
	WebMeter          float64

	PressPlate           string
	PressPlateGeneration uint64
	PressColor           string
	PressPhases          map[string]float64

	RegisterWindow *journal.RegisterWindowRecord

	DryerJobID       string
	DryerGeneration  uint64
	DryerTarget      float64
	DryerProfile     string
	DryerTemperature float64
	DryerActive      bool
}

func runtimeSaveSnapshot(dataDir string, values map[string]any) error {
	return journal.SaveSnapshot(filepath.Join(dataDir, "runtime-snapshot.json"), journal.Snapshot{Version: uint64(time.Now().UnixNano()), Values: values})
}

func runtimeLoadSnapshot(dataDir string) (journal.Snapshot, error) {
	return journal.LoadSnapshot(filepath.Join(dataDir, "runtime-snapshot.json"))
}

func decodeRuntimeSnapshot(snapshot journal.Snapshot) (RuntimeSnapshot, error) {
	decoded := RuntimeSnapshot{Version: snapshot.Version}
	data, err := json.Marshal(snapshot.Values)
	if err != nil {
		return RuntimeSnapshot{}, err
	}
	var raw struct {
		Job               model.Job                     `json:"job"`
		WebOwner          string                        `json:"web_owner"`
		WebJobGeneration  uint64                        `json:"web_job_generation"`
		WebRollGeneration uint64                        `json:"web_roll_generation"`
		WebThreaded       bool                          `json:"web_threaded"`
		WebMeter          float64                       `json:"web_meter"`
		PressPlate        string                        `json:"press_plate"`
		PressPlateGen     uint64                        `json:"press_plate_generation"`
		PressColor        string                        `json:"press_color"`
		PressPhases       map[string]float64            `json:"press_phases"`
		RegisterWindow    *journal.RegisterWindowRecord `json:"register_window"`
		DryerJobID        string                        `json:"dryer_job_id"`
		DryerGeneration   uint64                        `json:"dryer_generation"`
		DryerTarget       float64                       `json:"dryer_target"`
		DryerProfile      string                        `json:"dryer_profile"`
		DryerTemperature  float64                       `json:"dryer_temperature"`
		DryerActive       bool                          `json:"dryer_active"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return RuntimeSnapshot{}, fmt.Errorf("runtime snapshot is undecodable: %w", err)
	}
	decoded.Job = raw.Job
	decoded.WebOwner = raw.WebOwner
	decoded.WebJobGeneration = raw.WebJobGeneration
	decoded.WebRollGeneration = raw.WebRollGeneration
	decoded.WebThreaded = raw.WebThreaded
	decoded.WebMeter = raw.WebMeter
	decoded.PressPlate = raw.PressPlate
	decoded.PressPlateGeneration = raw.PressPlateGen
	decoded.PressColor = raw.PressColor
	decoded.PressPhases = raw.PressPhases
	decoded.RegisterWindow = raw.RegisterWindow
	decoded.DryerJobID = raw.DryerJobID
	decoded.DryerGeneration = raw.DryerGeneration
	decoded.DryerTarget = raw.DryerTarget
	decoded.DryerProfile = raw.DryerProfile
	decoded.DryerTemperature = raw.DryerTemperature
	decoded.DryerActive = raw.DryerActive
	return decoded, nil
}
