package hostedcontroller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	MaxAssignments          int
	ExcludedClusterIDs      map[string]struct{}
	ConnectorNamespace      string
	CentralEndpoint         string
	CentralServiceNamespace string
	SourceName              string
	SourceNamespace         string
	BootstrapAPIServer      string
	AgentImageRepo          string
	AgentImageTag           string
	AgentImageDigest        string
	AdvertisedRoutes        []string
	PollInterval            time.Duration
}

var sha256DigestRE = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func (c Config) Validate() error {
	if c.MaxAssignments <= 0 || c.MaxAssignments > 1000 {
		return errors.New("assignment cap must be between 1 and 1000")
	}
	if c.ConnectorNamespace == "" || c.CentralEndpoint == "" || c.CentralServiceNamespace == "" || c.SourceName == "" || c.SourceNamespace == "" || c.BootstrapAPIServer == "" || c.AgentImageRepo == "" || c.AgentImageTag == "" {
		return errors.New("hosted controller configuration is incomplete")
	}
	if !sha256DigestRE.MatchString(c.AgentImageDigest) {
		return errors.New("agent image digest must be sha256 followed by 64 lowercase hex characters")
	}
	if c.PollInterval < time.Second || c.PollInterval > time.Hour {
		return errors.New("poll interval must be between 1s and 1h")
	}
	return nil
}

type Reconciler struct {
	Central Central
	Kube    kubernetes.Interface
	Dynamic dynamic.Interface
	Config  Config
	Log     *slog.Logger
}

func (r *Reconciler) Reconcile(ctx context.Context) error {
	if r.Central == nil || r.Kube == nil || r.Dynamic == nil {
		return errors.New("hosted controller dependencies are required")
	}
	if err := r.Config.Validate(); err != nil {
		return err
	}
	inventory, err := r.Central.ListInventory(ctx)
	if err != nil {
		return fmt.Errorf("list hosted inventory: %w", err)
	}
	for _, row := range inventory {
		if _, excluded := r.Config.ExcludedClusterIDs[row.Cluster.ClusterID]; excluded {
			continue
		}
		if err := r.reconcileAssignment(ctx, row, ""); err != nil {
			return err
		}
	}
	requests, err := r.Central.ListRequests(ctx)
	if err != nil {
		return fmt.Errorf("list hosted requests: %w", err)
	}
	remaining := r.Config.MaxAssignments - len(inventory)
	for _, req := range requests {
		if remaining <= 0 {
			break
		}
		credential, err := r.Central.Assign(ctx, req.TenantID)
		if err != nil {
			return fmt.Errorf("assign hosted capacity for tenant %s: %w", req.TenantID, err)
		}
		row := InventoryRow{TenantID: credential.TenantID, Plan: req.Plan, Cluster: credential.Cluster}
		if err := r.reconcileAssignment(ctx, row, credential.ConnectorToken); err != nil {
			return err
		}
		remaining--
	}
	return nil
}

// Run is intentionally a single-writer polling loop. Resources absent from
// central inventory are not deleted; revocation is a separate destructive flow.
func (r *Reconciler) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		err := r.Reconcile(ctx)
		wait := r.Config.PollInterval
		if err != nil {
			r.logger().Error("hosted reconciliation failed", "error", err)
			wait = backoff
			if backoff < 30*time.Second {
				backoff *= 2
			}
		} else {
			backoff = time.Second
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

func (r *Reconciler) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}
