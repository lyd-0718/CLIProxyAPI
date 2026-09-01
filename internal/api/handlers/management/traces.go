package management

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/trace"
)

func (h *Handler) getTraceRecorder() *trace.Recorder {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.traceRecorder
}

// GetTraceSessions returns indexed sessions ordered by most recent activity.
func (h *Handler) GetTraceSessions(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusOK, gin.H{"sessions": []trace.SessionSummary{}, "total": 0, "enabled": false})
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	offset, _ := strconv.Atoi(c.Query("offset"))
	sessions, errList := recorder.ListSessionsFiltered(parseTraceFilter(c), limit, offset)
	if errList != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errList.Error()})
		return
	}
	total, errCount := recorder.CountSessionsFiltered(parseTraceFilter(c))
	if errCount != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errCount.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"sessions": sessions, "total": total, "enabled": true})
}

// GetTraceOverview returns aggregate request and token metrics for the chosen
// time window. It only reads the SQLite index; trace bodies stay on disk.
func (h *Handler) GetTraceOverview(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusOK, trace.Overview{Enabled: false, Models: []trace.Breakdown{}, APIKeys: []trace.Breakdown{}, Sources: []trace.Breakdown{}, Providers: []trace.Breakdown{}, Timeline: []trace.TimelineBucket{}})
		return
	}
	overview, errOverview := recorder.GetOverview(parseTraceFilter(c))
	if errOverview != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errOverview.Error()})
		return
	}
	c.JSON(http.StatusOK, overview)
}

// GetTraceRequests returns compact Turn rows for request-level browsing.
func (h *Handler) GetTraceRequests(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusOK, gin.H{"requests": []trace.TurnSummary{}, "total": 0, "enabled": false})
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit"))
	offset, _ := strconv.Atoi(c.Query("offset"))
	requests, errList := recorder.ListTurns(parseTraceFilter(c), limit, offset)
	if errList != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errList.Error()})
		return
	}
	total, errCount := recorder.CountTurns(parseTraceFilter(c))
	if errCount != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errCount.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"requests": requests, "total": total, "enabled": true})
}

func parseTraceFilter(c *gin.Context) trace.Filter {
	filter := trace.Filter{
		Query:    c.Query("q"),
		Model:    c.Query("model"),
		Provider: c.Query("provider"),
		APIKey:   c.Query("api_key"),
		Source:   c.Query("source"),
		Outcome:  c.Query("outcome"),
	}
	if value := strings.TrimSpace(c.Query("from")); value != "" {
		if parsed, errParse := time.Parse(time.RFC3339Nano, value); errParse == nil {
			filter.From = parsed
		}
	}
	if value := strings.TrimSpace(c.Query("to")); value != "" {
		if parsed, errParse := time.Parse(time.RFC3339Nano, value); errParse == nil {
			filter.To = parsed
		}
	}
	return filter
}

// GetTraceSession returns one session and its ordered events.
func (h *Handler) GetTraceSession(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace recorder disabled"})
		return
	}
	detail, errGet := recorder.GetSession(c.Param("session_id"))
	if errGet != nil {
		status := http.StatusInternalServerError
		if errors.Is(errGet, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errGet.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}

// GetTraceEvents returns only the event stream for a session.
func (h *Handler) GetTraceEvents(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace recorder disabled"})
		return
	}
	detail, errGet := recorder.GetSession(c.Param("session_id"))
	if errGet != nil {
		status := http.StatusInternalServerError
		if errors.Is(errGet, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errGet.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": detail.Events})
}

// GetTraceTurn returns events belonging to a single Turn.
func (h *Handler) GetTraceTurn(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace recorder disabled"})
		return
	}
	turnID := strings.TrimSpace(c.Param("turn_id"))
	rows, errQuery := recorder.ReadTurn(turnID)
	if errQuery != nil {
		status := http.StatusInternalServerError
		if errors.Is(errQuery, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errQuery.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"turn_id": turnID, "events": rows})
}

// ExportTraceSession returns the session's NDJSON representation.
func (h *Handler) ExportTraceSession(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace recorder disabled"})
		return
	}
	sessionID := c.Param("session_id")
	if _, errGet := recorder.GetSession(sessionID); errGet != nil {
		status := http.StatusInternalServerError
		if errors.Is(errGet, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errGet.Error()})
		return
	}
	payload, errExport := recorder.ExportSession(sessionID)
	if errExport != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errExport.Error()})
		return
	}
	c.Header("Content-Type", "application/x-ndjson")
	c.Header("Content-Disposition", `attachment; filename="trace.ndjson"`)
	c.Data(http.StatusOK, "application/x-ndjson", payload)
}

// DeleteTraceSession deletes one session and its files.
func (h *Handler) DeleteTraceSession(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil || !recorder.Enabled() {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace recorder disabled"})
		return
	}
	sessionID := c.Param("session_id")
	if _, errGet := recorder.GetSession(sessionID); errGet != nil {
		status := http.StatusInternalServerError
		if errors.Is(errGet, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errGet.Error()})
		return
	}
	if errDelete := recorder.DeleteSession(sessionID); errDelete != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errDelete.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

// GetTraceSettings returns recorder health and retention settings.
func (h *Handler) GetTraceSettings(c *gin.Context) {
	recorder := h.getTraceRecorder()
	if recorder == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	stats, errStats := recorder.Stats()
	if errStats != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errStats.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}

// PatchTraceSettings intentionally reports immutable settings in the first
// version. Configuration changes are applied on the next server restart.
func (h *Handler) PatchTraceSettings(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "trace settings are configured in config.yaml and require restart"})
}
