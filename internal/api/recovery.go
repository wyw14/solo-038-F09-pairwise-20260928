package api

import (
	"net/http"
	"time"

	"github.com/wyw14/solo-038/internal/service"
)

type RecoveryStatus struct {
	Healthy   bool                    `json:"healthy"`
	CheckedAt time.Time               `json:"checked_at"`
	Incidents []service.Incident      `json:"incidents"`
	Report    *service.RecoveryReport `json:"report,omitempty"`
}

func (handler *Handler) recoveryStatus(writer http.ResponseWriter, request *http.Request) {
	status := RecoveryStatus{Healthy: handler.runtime.Healthy(), CheckedAt: time.Now().UTC(), Incidents: handler.runtime.Incidents()}
	if report, ok := handler.runtime.RecoveryReport(); ok {
		status.Report = &report
	}
	writeJSON(writer, http.StatusOK, status)
}
