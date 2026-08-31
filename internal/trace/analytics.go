package trace

import (
	"strings"
	"time"
)

// Filter limits analytics and request queries to a common time and identity
// window. Empty string fields are intentionally treated as no filter.
type Filter struct {
	From   time.Time
	To     time.Time
	Query  string
	Model  string
	APIKey string
	Source string
}

// TurnSummary is the compact request/response view used by the management
// center's Requests tab.
type TurnSummary struct {
	TurnID          string    `json:"turn_id"`
	SessionID       string    `json:"session_id"`
	SessionSource   string    `json:"session_id_source"`
	RequestID       string    `json:"request_id"`
	TraceID         string    `json:"trace_id,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	LastAt          time.Time `json:"last_at"`
	Status          int       `json:"status"`
	Model           string    `json:"model,omitempty"`
	Provider        string    `json:"provider,omitempty"`
	APIKey          string    `json:"api_key,omitempty"`
	InputTokens     int64     `json:"input_tokens"`
	OutputTokens    int64     `json:"output_tokens"`
	ReasoningTokens int64     `json:"reasoning_tokens"`
	CachedTokens    int64     `json:"cached_tokens"`
	TotalTokens     int64     `json:"total_tokens"`
	LatencyMS       int64     `json:"latency_ms"`
	TTFTMS          int64     `json:"ttft_ms"`
	Failed          bool      `json:"failed"`
	Incomplete      bool      `json:"incomplete"`
}

// Breakdown is a token and request aggregate grouped by one dimension.
type Breakdown struct {
	Name            string `json:"name"`
	Turns           int64  `json:"turns"`
	Failed          int64  `json:"failed"`
	Incomplete      int64  `json:"incomplete"`
	InputTokens     int64  `json:"input_tokens"`
	OutputTokens    int64  `json:"output_tokens"`
	ReasoningTokens int64  `json:"reasoning_tokens"`
	CachedTokens    int64  `json:"cached_tokens"`
	TotalTokens     int64  `json:"total_tokens"`
}

// TimelineBucket is an hourly UTC request bucket for a compact activity chart.
type TimelineBucket struct {
	At         time.Time `json:"at"`
	Turns      int64     `json:"turns"`
	Failed     int64     `json:"failed"`
	TotalTokens int64    `json:"total_tokens"`
}

// Overview is the aggregate data for a selected time window.
type Overview struct {
	Enabled         bool             `json:"enabled"`
	From            time.Time       `json:"from,omitempty"`
	To              time.Time       `json:"to,omitempty"`
	Sessions        int64            `json:"sessions"`
	Turns           int64            `json:"turns"`
	Events          int64            `json:"events"`
	Failed          int64            `json:"failed"`
	Incomplete      int64            `json:"incomplete"`
	InputTokens     int64            `json:"input_tokens"`
	OutputTokens    int64            `json:"output_tokens"`
	ReasoningTokens int64            `json:"reasoning_tokens"`
	CachedTokens    int64            `json:"cached_tokens"`
	TotalTokens     int64            `json:"total_tokens"`
	AvgLatencyMS    float64          `json:"avg_latency_ms"`
	AvgTTFTMS       float64          `json:"avg_ttft_ms"`
	Models          []Breakdown      `json:"models"`
	APIKeys         []Breakdown      `json:"api_keys"`
	Sources         []Breakdown      `json:"sources"`
	Timeline        []TimelineBucket `json:"timeline"`
}

// ListTurns returns indexed request turns ordered by most recent activity.
func (r *Recorder) ListTurns(filter Filter, limit, offset int) ([]TurnSummary, error) {
	if !r.Enabled() {
		return []TurnSummary{}, nil
	}
	limit, offset = normalizePage(limit, offset, 200)
	args, where := turnFilterSQL(filter)
	rows, errQuery := r.db.Query(`SELECT t.turn_id,t.session_id,s.session_source,t.request_id,t.trace_id,t.started_at,t.last_at,t.status,t.model,t.provider,t.api_key,
		t.input_tokens,t.output_tokens,t.reasoning_tokens,t.cached_tokens,t.total_tokens,t.latency_ms,t.ttft_ms,t.failed,t.incomplete
		FROM turns t JOIN sessions s ON s.session_id=t.session_id `+where+` ORDER BY t.last_at DESC LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if errQuery != nil {
		return nil, errQuery
	}
	defer rows.Close()
	result := make([]TurnSummary, 0)
	for rows.Next() {
		item, errScan := scanTurn(rows)
		if errScan != nil {
			return nil, errScan
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// GetOverview computes aggregate metrics from the SQLite index. Trace bodies
// remain in NDJSON and are never loaded for this operation.
func (r *Recorder) GetOverview(filter Filter) (Overview, error) {
	if !r.Enabled() {
		return Overview{Enabled: false, Models: []Breakdown{}, APIKeys: []Breakdown{}, Sources: []Breakdown{}, Timeline: []TimelineBucket{}}, nil
	}
	args, where := turnFilterSQL(filter)
	var out Overview
	out.Enabled = true
	out.From = filter.From
	out.To = filter.To
	if err := r.db.QueryRow(`SELECT COUNT(DISTINCT t.session_id),COUNT(*),COALESCE(SUM(t.failed),0),COALESCE(SUM(t.incomplete),0),
		COALESCE(SUM(t.input_tokens),0),COALESCE(SUM(t.output_tokens),0),COALESCE(SUM(t.reasoning_tokens),0),COALESCE(SUM(t.cached_tokens),0),COALESCE(SUM(t.total_tokens),0),
		COALESCE(AVG(NULLIF(t.latency_ms,0)),0),COALESCE(AVG(NULLIF(t.ttft_ms,0)),0)
		FROM turns t JOIN sessions s ON s.session_id=t.session_id `+where, args...).Scan(&out.Sessions, &out.Turns, &out.Failed, &out.Incomplete,
		&out.InputTokens, &out.OutputTokens, &out.ReasoningTokens, &out.CachedTokens, &out.TotalTokens, &out.AvgLatencyMS, &out.AvgTTFTMS); err != nil {
		return Overview{}, err
	}
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM events e JOIN turns t ON t.turn_id=e.turn_id JOIN sessions s ON s.session_id=t.session_id `+where, args...).Scan(&out.Events); err != nil {
		return Overview{}, err
	}
	var err error
	if out.Models, err = r.breakdown(filter, where, args, "model"); err != nil {
		return Overview{}, err
	}
	if out.APIKeys, err = r.breakdown(filter, where, args, "api_key"); err != nil {
		return Overview{}, err
	}
	if out.Sources, err = r.breakdown(filter, where, args, "session_source"); err != nil {
		return Overview{}, err
	}
	if out.Timeline, err = r.timeline(filter, where, args); err != nil {
		return Overview{}, err
	}
	return out, nil
}

func normalizePage(limit, offset, defaultLimit int) (int, int) {
	if limit <= 0 || limit > 500 {
		limit = defaultLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func turnFilterSQL(filter Filter) ([]any, string) {
	args := make([]any, 0, 16)
	conditions := make([]string, 0, 6)
	needle := strings.TrimSpace(filter.Query)
	conditions = append(conditions, "(? = '' OR t.turn_id LIKE ? OR t.session_id LIKE ? OR t.request_id LIKE ? OR t.model LIKE ? OR t.provider LIKE ?)")
	like := "%" + needle + "%"
	args = append(args, needle, like, like, like, like, like)
	model := strings.TrimSpace(filter.Model)
	conditions = append(conditions, "(? = '' OR t.model LIKE ?)")
	args = append(args, model, "%"+model+"%")
	apiKey := strings.TrimSpace(filter.APIKey)
	conditions = append(conditions, "(? = '' OR t.api_key LIKE ?)")
	args = append(args, apiKey, "%"+apiKey+"%")
	source := strings.TrimSpace(filter.Source)
	conditions = append(conditions, "(? = '' OR s.session_source LIKE ?)")
	args = append(args, source, "%"+source+"%")
	if !filter.From.IsZero() {
		conditions = append(conditions, "t.last_at >= ?")
		args = append(args, filter.From.UTC().Format(time.RFC3339Nano))
	}
	if !filter.To.IsZero() {
		conditions = append(conditions, "t.last_at <= ?")
		args = append(args, filter.To.UTC().Format(time.RFC3339Nano))
	}
	return args, "WHERE " + strings.Join(conditions, " AND ")
}

type scanner interface {
	Scan(...any) error
}

func scanTurn(row scanner) (TurnSummary, error) {
	var item TurnSummary
	var started, last string
	var failed, incomplete int
	err := row.Scan(&item.TurnID, &item.SessionID, &item.SessionSource, &item.RequestID, &item.TraceID, &started, &last, &item.Status,
		&item.Model, &item.Provider, &item.APIKey, &item.InputTokens, &item.OutputTokens, &item.ReasoningTokens, &item.CachedTokens,
		&item.TotalTokens, &item.LatencyMS, &item.TTFTMS, &failed, &incomplete)
	if err != nil {
		return TurnSummary{}, err
	}
	item.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
	item.LastAt, _ = time.Parse(time.RFC3339Nano, last)
	item.Failed = failed != 0
	item.Incomplete = incomplete != 0
	return item, nil
}

func (r *Recorder) breakdown(filter Filter, where string, args []any, dimension string) ([]Breakdown, error) {
	column := "t." + dimension
	if dimension == "session_source" {
		column = "s.session_source"
	}
	rows, errQuery := r.db.Query(`SELECT COALESCE(NULLIF(`+column+`,''),'(unknown)'),COUNT(*),COALESCE(SUM(t.failed),0),COALESCE(SUM(t.incomplete),0),
		COALESCE(SUM(t.input_tokens),0),COALESCE(SUM(t.output_tokens),0),COALESCE(SUM(t.reasoning_tokens),0),COALESCE(SUM(t.cached_tokens),0),COALESCE(SUM(t.total_tokens),0)
		FROM turns t JOIN sessions s ON s.session_id=t.session_id `+where+` GROUP BY `+column+` ORDER BY COUNT(*) DESC,`+column+` LIMIT 100`, args...)
	if errQuery != nil {
		return nil, errQuery
	}
	defer rows.Close()
	result := make([]Breakdown, 0)
	for rows.Next() {
		var item Breakdown
		if errScan := rows.Scan(&item.Name, &item.Turns, &item.Failed, &item.Incomplete, &item.InputTokens, &item.OutputTokens, &item.ReasoningTokens, &item.CachedTokens, &item.TotalTokens); errScan != nil {
			return nil, errScan
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *Recorder) timeline(filter Filter, where string, args []any) ([]TimelineBucket, error) {
	rows, errQuery := r.db.Query(`SELECT substr(t.last_at,1,13)||':00:00Z',COUNT(*),COALESCE(SUM(t.failed),0),COALESCE(SUM(t.total_tokens),0)
		FROM turns t JOIN sessions s ON s.session_id=t.session_id `+where+` GROUP BY substr(t.last_at,1,13) ORDER BY substr(t.last_at,1,13)`, args...)
	if errQuery != nil {
		return nil, errQuery
	}
	defer rows.Close()
	result := make([]TimelineBucket, 0)
	for rows.Next() {
		var item TimelineBucket
		var at string
		if errScan := rows.Scan(&at, &item.Turns, &item.Failed, &item.TotalTokens); errScan != nil {
			return nil, errScan
		}
		item.At, _ = time.Parse(time.RFC3339, at)
		result = append(result, item)
	}
	return result, rows.Err()
}
