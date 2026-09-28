package service

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type Incident struct {
	ID        model.EventID `json:"id"`
	Severity  string        `json:"severity"`
	Component string        `json:"component"`
	Message   string        `json:"message"`
	At        time.Time     `json:"at"`
}

var incidentMu sync.Mutex

func (runtime *Runtime) RecordIncident(severity, component, message string) Incident {
	incidentMu.Lock()
	defer incidentMu.Unlock()
	incident := Incident{ID: model.NewEventID(), Severity: severity, Component: component, Message: message, At: time.Now().UTC()}
	runtime.incidents = append(runtime.incidents, incident)
	return incident
}

func (runtime *Runtime) Incidents() []Incident {
	incidentMu.Lock()
	defer incidentMu.Unlock()
	values := append([]Incident(nil), runtime.incidents...)
	sort.Slice(values, func(left, right int) bool { return values[left].At.After(values[right].At) })
	return values
}

// clearRecoveryIncidents removes incidents produced by an earlier recovery
// fold so replaying the same events does not accumulate duplicates.
func (runtime *Runtime) clearRecoveryIncidents() {
	incidentMu.Lock()
	defer incidentMu.Unlock()
	filtered := runtime.incidents[:0]
	for _, incident := range runtime.incidents {
		if !strings.HasPrefix(incident.Message, recoveryIncidentPrefix) {
			filtered = append(filtered, incident)
		}
	}
	runtime.incidents = filtered
}
