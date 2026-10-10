package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresPersister is the factory's durable, ciphertext-only persistence
// backend. MemoryStore remains the read source of truth while it writes here.
type PostgresPersister struct {
	pool *pgxpool.Pool
}

const factorySchema = `
CREATE TABLE IF NOT EXISTS boxes (
  login_server TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, hostname TEXT NOT NULL DEFAULT '',
  backend TEXT NOT NULL DEFAULT '', backend_id TEXT NOT NULL DEFAULT '', hs_user TEXT NOT NULL DEFAULT '',
  api_key_enc BYTEA, status TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL,
  last_validated_at TIMESTAMPTZ, consecutive_failures INTEGER NOT NULL DEFAULT 0, replaced_by TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, login_server TEXT NOT NULL, kind TEXT NOT NULL,
  status TEXT NOT NULL, idempotency_key TEXT, request_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, last_error TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_tenant_idempotency_key_idx ON jobs (tenant_id, idempotency_key);
CREATE TABLE IF NOT EXISTS policy_state (
  tenant_id TEXT PRIMARY KEY, pushed_routes JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_push_at TIMESTAMPTZ, last_error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS audit (
  id BIGSERIAL PRIMARY KEY, at TIMESTAMPTZ NOT NULL, actor TEXT NOT NULL DEFAULT '',
  tenant_id TEXT NOT NULL, action TEXT NOT NULL DEFAULT ''
);
`

// NewPostgresPersister opens a pool and applies the idempotent factory schema.
func NewPostgresPersister(ctx context.Context, dsn string) (*PostgresPersister, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("factory store: connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("factory store: ping postgres: %w", err)
	}
	p := &PostgresPersister{pool: pool}
	if _, err := pool.Exec(ctx, factorySchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("factory store: ensure schema: %w", err)
	}
	return p, nil
}

func persisterContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (p *PostgresPersister) UpsertBox(box Box) error {
	ctx, cancel := persisterContext()
	defer cancel()
	_, err := p.pool.Exec(ctx, `INSERT INTO boxes
 (login_server, tenant_id, hostname, backend, backend_id, hs_user, api_key_enc, status, created_at, last_validated_at, consecutive_failures, replaced_by)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
 ON CONFLICT (login_server) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, hostname=EXCLUDED.hostname,
 backend=EXCLUDED.backend, backend_id=EXCLUDED.backend_id, hs_user=EXCLUDED.hs_user, api_key_enc=EXCLUDED.api_key_enc,
 status=EXCLUDED.status, created_at=EXCLUDED.created_at, last_validated_at=EXCLUDED.last_validated_at,
 consecutive_failures=EXCLUDED.consecutive_failures, replaced_by=EXCLUDED.replaced_by`,
		box.LoginServer, box.TenantID, box.Hostname, box.Backend, box.BackendID, box.HSUser, box.apiKeyEnc, box.Status,
		box.CreatedAt, nullableTime(box.LastValidatedAt), box.ConsecutiveFailures, box.ReplacedBy)
	if err != nil {
		return fmt.Errorf("factory store: upsert box %s: %w", box.LoginServer, err)
	}
	return nil
}

func (p *PostgresPersister) DeleteBox(loginServer string) error {
	ctx, cancel := persisterContext()
	defer cancel()
	if _, err := p.pool.Exec(ctx, `DELETE FROM boxes WHERE login_server = $1`, loginServer); err != nil {
		return fmt.Errorf("factory store: delete box %s: %w", loginServer, err)
	}
	return nil
}

func (p *PostgresPersister) UpsertPolicyState(state PolicyState) error {
	routes, err := json.Marshal(state.PushedRoutes)
	if err != nil {
		return fmt.Errorf("factory store: marshal policy state: %w", err)
	}
	ctx, cancel := persisterContext()
	defer cancel()
	_, err = p.pool.Exec(ctx, `INSERT INTO policy_state (tenant_id, pushed_routes, last_push_at, last_error)
 VALUES ($1,$2,$3,$4)
 ON CONFLICT (tenant_id) DO UPDATE SET pushed_routes=EXCLUDED.pushed_routes, last_push_at=EXCLUDED.last_push_at, last_error=EXCLUDED.last_error`,
		state.TenantID, routes, nullableTime(state.LastPushAt), state.LastError)
	if err != nil {
		return fmt.Errorf("factory store: upsert policy state %s: %w", state.TenantID, err)
	}
	return nil
}

func (p *PostgresPersister) UpsertJob(job Job) error {
	ctx, cancel := persisterContext()
	defer cancel()
	_, err := p.pool.Exec(ctx, `INSERT INTO jobs
 (id, tenant_id, login_server, kind, status, idempotency_key, request_hash, created_at, updated_at, last_error)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
 ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, login_server=EXCLUDED.login_server, kind=EXCLUDED.kind,
 status=EXCLUDED.status, idempotency_key=EXCLUDED.idempotency_key, request_hash=EXCLUDED.request_hash,
 created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at, last_error=EXCLUDED.last_error`,
		job.ID, job.TenantID, job.LoginServer, job.Kind, job.Status, nullableString(job.IdempotencyKey), job.RequestHash,
		job.CreatedAt, job.UpdatedAt, job.LastError)
	if err != nil {
		return fmt.Errorf("factory store: upsert job %s: %w", job.ID, err)
	}
	return nil
}

func (p *PostgresPersister) AppendAudit(audit Audit) error {
	ctx, cancel := persisterContext()
	defer cancel()
	if _, err := p.pool.Exec(ctx, `INSERT INTO audit (at, actor, tenant_id, action) VALUES ($1,$2,$3,$4)`, audit.At, audit.Actor, audit.TenantID, audit.Action); err != nil {
		return fmt.Errorf("factory store: append audit: %w", err)
	}
	return nil
}

func (p *PostgresPersister) Close() { p.pool.Close() }

type storeSnapshot struct {
	boxes        []Box
	policyStates []PolicyState
	jobs         []Job
	audits       []Audit
}

func (p *PostgresPersister) loadAll(ctx context.Context) (*storeSnapshot, error) {
	snapshot := &storeSnapshot{}
	rows, err := p.pool.Query(ctx, `SELECT tenant_id, login_server, hostname, backend, backend_id, hs_user, api_key_enc, status, created_at, last_validated_at, consecutive_failures, replaced_by FROM boxes`)
	if err != nil {
		return nil, fmt.Errorf("factory store: load boxes: %w", err)
	}
	for rows.Next() {
		var box Box
		var validated *time.Time
		if err := rows.Scan(&box.TenantID, &box.LoginServer, &box.Hostname, &box.Backend, &box.BackendID, &box.HSUser, &box.apiKeyEnc, &box.Status, &box.CreatedAt, &validated, &box.ConsecutiveFailures, &box.ReplacedBy); err != nil {
			rows.Close()
			return nil, fmt.Errorf("factory store: scan box: %w", err)
		}
		if validated != nil {
			box.LastValidatedAt = *validated
		}
		snapshot.boxes = append(snapshot.boxes, box)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("factory store: load boxes: %w", err)
	}
	rows.Close()
	rows, err = p.pool.Query(ctx, `SELECT tenant_id, pushed_routes, last_push_at, last_error FROM policy_state`)
	if err != nil {
		return nil, fmt.Errorf("factory store: load policy state: %w", err)
	}
	for rows.Next() {
		var state PolicyState
		var routes []byte
		var pushed *time.Time
		if err := rows.Scan(&state.TenantID, &routes, &pushed, &state.LastError); err != nil {
			rows.Close()
			return nil, fmt.Errorf("factory store: scan policy state: %w", err)
		}
		if err := json.Unmarshal(routes, &state.PushedRoutes); err != nil {
			rows.Close()
			return nil, fmt.Errorf("factory store: decode policy state: %w", err)
		}
		if pushed != nil {
			state.LastPushAt = *pushed
		}
		snapshot.policyStates = append(snapshot.policyStates, state)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("factory store: load policy state: %w", err)
	}
	rows.Close()
	rows, err = p.pool.Query(ctx, `SELECT id, tenant_id, login_server, kind, status, idempotency_key, request_hash, created_at, updated_at, last_error FROM jobs`)
	if err != nil {
		return nil, fmt.Errorf("factory store: load jobs: %w", err)
	}
	for rows.Next() {
		var job Job
		var idempotencyKey *string
		if err := rows.Scan(&job.ID, &job.TenantID, &job.LoginServer, &job.Kind, &job.Status, &idempotencyKey, &job.RequestHash, &job.CreatedAt, &job.UpdatedAt, &job.LastError); err != nil {
			rows.Close()
			return nil, fmt.Errorf("factory store: scan job: %w", err)
		}
		if idempotencyKey != nil {
			job.IdempotencyKey = *idempotencyKey
		}
		snapshot.jobs = append(snapshot.jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("factory store: load jobs: %w", err)
	}
	rows.Close()
	rows, err = p.pool.Query(ctx, `SELECT at, actor, tenant_id, action FROM audit ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("factory store: load audit: %w", err)
	}
	for rows.Next() {
		var audit Audit
		if err := rows.Scan(&audit.At, &audit.Actor, &audit.TenantID, &audit.Action); err != nil {
			rows.Close()
			return nil, fmt.Errorf("factory store: scan audit: %w", err)
		}
		snapshot.audits = append(snapshot.audits, audit)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("factory store: load audit: %w", err)
	}
	rows.Close()
	return snapshot, nil
}

// LoadFrom restores durable records and rebuilds the derived fabric indexes.
// The persister must provide the factory Postgres loading extension.
func (s *MemoryStore) LoadFrom(ctx context.Context, persister Persister) error {
	loader, ok := persister.(interface {
		loadAll(context.Context) (*storeSnapshot, error)
	})
	if !ok {
		return errors.New("factory store: persister does not support loading")
	}
	snapshot, err := loader.loadAll(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.boxes, s.policyStates, s.jobs = make(map[string]Box), make(map[string]PolicyState), make(map[string]Job)
	s.fabricJobs, s.idempotency, s.audits = make(map[string]string), make(map[string]string), make(map[string][]Audit)
	for _, box := range snapshot.boxes {
		s.boxes[box.LoginServer] = cloneBox(box)
	}
	for _, state := range snapshot.policyStates {
		s.policyStates[state.TenantID] = clonePolicyState(state)
	}
	for _, job := range snapshot.jobs {
		s.jobs[job.ID] = job
		if job.IdempotencyKey != "" {
			s.idempotency[job.TenantID+"\x00"+job.IdempotencyKey] = job.ID
		}
		if job.Kind == "provision" {
			if box, ok := s.boxes[job.LoginServer]; ok && box.Status != StatusDead {
				s.fabricJobs[job.TenantID] = job.ID
			}
		}
	}
	for _, audit := range snapshot.audits {
		s.audits[audit.TenantID] = append(s.audits[audit.TenantID], audit)
	}
	s.persister = persister
	return nil
}

var _ Persister = (*PostgresPersister)(nil)
