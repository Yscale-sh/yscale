// Signup storage backends. The platform injects DATABASE_URL when the
// deploy.yaml db block is declared; without it the JSONL file store is used,
// exactly as before. Both backends serve the same JSONL wire format on
// export so downstream tooling cannot tell them apart.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
)

// store persists signup records. Records are append-only; readAll returns
// them in insertion order (foldEffective depends on that).
type store interface {
	append(rec signupRecord) error
	readAll() ([]signupRecord, error)
}

// foldEffective folds the append-only log: the LAST record per email wins,
// and an unsubscribe tombstone removes the address from the output.
// Survivors keep the position of their winning record.
func foldEffective(recs []signupRecord) []signupRecord {
	latest := map[string]int{}
	for i, rec := range recs {
		if rec.Action == "unsubscribe" {
			delete(latest, rec.Email)
			continue
		}
		latest[rec.Email] = i
	}
	keep := make([]int, 0, len(latest))
	for _, i := range latest {
		keep = append(keep, i)
	}
	sort.Ints(keep)
	out := make([]signupRecord, 0, len(keep))
	for _, i := range keep {
		out = append(out, recs[i])
	}
	return out
}

// fileStore is the JSONL backend: one JSON record per line in an append-only
// file, writes guarded by a mutex. A nil file means the path was unwritable;
// appends become no-ops (slog is the backup store).
type fileStore struct {
	path string
	mu   sync.Mutex // guards writes to file
	file *os.File   // opened O_APPEND; nil if unwritable
}

func (s *fileStore) append(rec signupRecord) error {
	if s.file == nil {
		return nil
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.file.Write(line)
	return err
}

func (s *fileStore) readAll() ([]signupRecord, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no signups yet
		}
		return nil, err
	}
	defer f.Close()
	var recs []signupRecord
	dec := json.NewDecoder(f)
	for dec.More() {
		var rec signupRecord
		if err := dec.Decode(&rec); err != nil {
			break // tolerate a torn tail line
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

const pgTimeout = 10 * time.Second

// pgStore keeps signups in Postgres. Rows mirror the JSONL record shape and
// id preserves insertion order, so export re-serializes to the same lines
// the file store would produce.
type pgStore struct {
	db *sql.DB
}

// newPGStore connects and runs the idempotent migration. Errors are returned
// so main can fall back to the file store instead of crash-looping the
// marketing site over a managed dependency.
func newPGStore(databaseURL string) (*pgStore, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), pgTimeout)
	defer cancel()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS signups (
		id BIGSERIAL PRIMARY KEY,
		ts timestamptz NOT NULL,
		email text NOT NULL,
		track text NOT NULL DEFAULT '',
		company text NOT NULL DEFAULT '',
		action text NOT NULL DEFAULT '',
		ip text NOT NULL DEFAULT '',
		ua text NOT NULL DEFAULT ''
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &pgStore{db: db}, nil
}

func (s *pgStore) append(rec signupRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), pgTimeout)
	defer cancel()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO signups (ts, email, track, company, action, ip, ua)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rec.Ts, rec.Email, rec.Track, rec.Company, rec.Action, rec.IP, rec.UA)
	return err
}

func (s *pgStore) readAll() ([]signupRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pgTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx,
		`SELECT ts, email, track, company, action, ip, ua FROM signups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recs []signupRecord
	for rows.Next() {
		var rec signupRecord
		var ts time.Time
		if err := rows.Scan(&ts, &rec.Email, &rec.Track, &rec.Company, &rec.Action, &rec.IP, &rec.UA); err != nil {
			return nil, err
		}
		rec.Ts = ts.UTC().Format(time.RFC3339)
		recs = append(recs, rec)
	}
	return recs, rows.Err()
}
