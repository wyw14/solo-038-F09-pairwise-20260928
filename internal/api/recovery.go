package api

import (
	"net/http"
	"time"

	"github.com/wyw14/solo-038/internal/service"
)

type RecoveryStatus struct {
	Healthy   bool                   `json:"healthy"`
	CheckedAt time.Time              `json:"checked_at"`
	Incidents []service.Incident     `json:"incidents"`
	Restart   *service.RestartReport `json:"restart,omitempty"`
}

func (handler *Handler) recoveryStatus(writer http.ResponseWriter, request *http.Request) {
	status := RecoveryStatus{Healthy: handler.runtime.Healthy(), CheckedAt: time.Now().UTC(), Incidents: handler.runtime.Incidents()}
	if report, ok := handler.runtime.RestartReport(); ok {
		status.Restart = &report
	}
	writeJSON(writer, http.StatusOK, status)
}

// restartRecovery re-runs restart recovery on demand (e.g. after an operator
// repaired the journal or replaced a roll). The line stays stopped; the
// response is the per-item verdict.
func (handler *Handler) restartRecovery(writer http.ResponseWriter, request *http.Request) {
	report, err := handler.runtime.RestartRecover()
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}
