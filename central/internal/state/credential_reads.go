package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type credentialKind uint8

const (
	tenantCredential credentialKind = iota
	clusterCredential
	publisherCredential
)

type credentialReader interface {
	readCredential(context.Context, string, credentialKind) (*Customer, string, error)
}

// Shared credential resolution needs this verifier even in an OSS export
// where the publisher management service is excluded. Keep its stored format.
func HashCatalogPublisherCredential(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Context-aware authentication reads committed credentials and current tenant
// policy when a durable backend is configured. Reads never hydrate mutation
// maps, and backend failure never permits a stale-cache authentication.
func (s *Store) AuthCustomerContext(ctx context.Context, token string) (*Customer, error) {
	c, _, err := s.resolveCredential(ctx, token, tenantCredential)
	return c, err
}

func (s *Store) AuthClusterCredentialContext(ctx context.Context, token string) (*Customer, string, error) {
	return s.resolveCredential(ctx, token, clusterCredential)
}

func (s *Store) AuthCatalogPublisherContext(ctx context.Context, token string) (*Customer, string, error) {
	return s.resolveCredential(ctx, token, publisherCredential)
}

func (s *Store) resolveCredential(ctx context.Context, token string, kind credentialKind) (*Customer, string, error) {
	if token == "" {
		return nil, "", ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	var digest string
	switch kind {
	case tenantCredential:
		digest = HashCustomerToken(token)
	case clusterCredential:
		digest = HashClusterCredential(token)
	case publisherCredential:
		digest = HashCatalogPublisherCredential(token)
	default:
		return nil, "", fmt.Errorf("%w: unknown credential kind", ErrPersistence)
	}
	s.mu.RLock()
	if p := s.persist; p != nil {
		s.mu.RUnlock()
		reader, ok := p.(credentialReader)
		if !ok {
			return nil, "", fmt.Errorf("%w: durable credential reader unavailable", ErrPersistence)
		}
		return reader.readCredential(ctx, digest, kind)
	}
	defer s.mu.RUnlock()
	var c *Customer
	switch kind {
	case tenantCredential:
		c = s.customersByTok[digest]
	case clusterCredential:
		c = s.customers[s.clusterCreds[digest].customerID]
	case publisherCredential:
		c = s.customers[s.catalogPublisherCreds[digest].customerID]
	}
	if c == nil || s.tombstoned[c.ID] {
		return nil, "", ErrNotFound
	}
	bindings := credentialBindings(c, digest, kind)
	if len(bindings) != 1 {
		return nil, "", ErrNotFound
	}
	return c, bindings[0], nil
}

// Recheck decoded data as well as its lookup predicate. A credential must name
// exactly one active principal, never an arbitrary duplicate or empty scope.
func credentialBindings(c *Customer, digest string, kind credentialKind) []string {
	if c == nil || c.ID == "" || c.Revoked() {
		return nil
	}
	var bindings []string
	switch kind {
	case tenantCredential:
		if c.TokenHash == digest {
			bindings = append(bindings, "")
		}
	case clusterCredential:
		for _, cluster := range c.RegisteredClusters {
			if cluster != nil && cluster.CredentialHash == digest && cluster.ClusterID != "" {
				bindings = append(bindings, cluster.ClusterID)
			}
		}
	case publisherCredential:
		for _, publisher := range c.CatalogPublishers {
			if publisher != nil && publisher.CredentialHash == digest && publisher.ID != "" {
				bindings = append(bindings, publisher.ID)
			}
		}
	}
	return bindings
}

func (p *pgPersister) readCredential(ctx context.Context, digest string, kind credentialKind) (*Customer, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	projection, join, predicate := "''", "", "c.data->>'TokenHash' = $1"
	switch kind {
	case tenantCredential:
	case clusterCredential, publisherCredential:
		field, idField := "RegisteredClusters", "ClusterID"
		if kind == publisherCredential {
			field, idField = "CatalogPublishers", "ID"
		}
		// The containment predicate uses the matching GIN expression index.
		// JSON null or malformed non-array fields cannot become an auth grant.
		join = fmt.Sprintf(`CROSS JOIN LATERAL jsonb_array_elements(
			CASE WHEN jsonb_typeof(c.data->'%s') = 'array' THEN c.data->'%s' ELSE '[]'::jsonb END) principal`, field, field)
		projection = fmt.Sprintf("principal->>'%s'", idField)
		predicate = fmt.Sprintf("c.data->'%s' @> jsonb_build_array(jsonb_build_object('CredentialHash', $1::text)) AND principal->>'CredentialHash' = $1", field)
	default:
		return nil, "", fmt.Errorf("%w: unknown credential kind", ErrPersistence)
	}
	// All interpolated names are constants. Only the verifier, never the
	// plaintext bearer, crosses this boundary. LIMIT 2 detects ambiguous keys.
	rows, err := p.pool.Query(ctx, fmt.Sprintf(`SELECT c.id, c.data, %s FROM %s c %s
		WHERE (%s) AND (c.data->'RevokedAt' IS NULL OR c.data->'RevokedAt' = 'null'::jsonb)
		AND NOT EXISTS (SELECT 1 FROM %s tombstone WHERE tombstone.id = c.id)
		LIMIT 2`, projection, tblCustomers, join, predicate, tblTombstones), digest)
	if err != nil {
		return nil, "", fmt.Errorf("%w: read credential: %w", ErrPersistence, err)
	}
	defer rows.Close()
	var id, binding string
	var data []byte
	count := 0
	for rows.Next() {
		var scope *string
		if err := rows.Scan(&id, &data, &scope); err != nil {
			return nil, "", fmt.Errorf("%w: scan credential: %w", ErrPersistence, err)
		}
		binding = ""
		if scope != nil {
			binding = *scope
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("%w: read credential: %w", ErrPersistence, err)
	}
	if count != 1 {
		return nil, "", ErrNotFound
	}
	var c Customer
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, "", fmt.Errorf("%w: decode credential owner: %w", ErrPersistence, err)
	}
	if c.ID != id {
		return nil, "", fmt.Errorf("%w: credential owner identity mismatch", ErrPersistence)
	}
	bindings := credentialBindings(&c, digest, kind)
	if len(bindings) != 1 || bindings[0] != binding {
		return nil, "", ErrNotFound
	}
	if c.Token != "" || (c.Mesh != nil && c.Mesh.APIKey != "") {
		return nil, "", fmt.Errorf("%w: credential row requires startup migration", ErrPersistence)
	}
	rewrite, err := p.prepareLoadedCustomerCredential(&c)
	if err != nil {
		return nil, "", fmt.Errorf("%w: prepare credential owner: %w", ErrPersistence, err)
	}
	if rewrite {
		return nil, "", fmt.Errorf("%w: credential row requires startup migration", ErrPersistence)
	}
	return &c, binding, nil
}
