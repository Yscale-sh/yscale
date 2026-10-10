package state

// WorkloadActionPrincipal is supplied by authenticated server code, never by
// the request body. CredentialHash is the verifier observed at authentication;
// a human cancellation supplies only AccountID and checks current membership.
// Connector lifecycle reports never accept a human principal.
type WorkloadActionPrincipal struct {
	AccountID      string
	ClusterID      string
	CredentialHash string
}

func workloadCredentialAuthorized(c *Customer, w *Workload, principal WorkloadActionPrincipal) bool {
	if c == nil || w == nil || principal.AccountID != "" || principal.CredentialHash == "" {
		return false
	}
	kind := tenantCredential
	if principal.ClusterID != "" {
		kind = clusterCredential
	}
	bindings := credentialBindings(c, principal.CredentialHash, kind)
	if len(bindings) != 1 || bindings[0] != principal.ClusterID {
		return false
	}
	return principal.ClusterID == "" || w.ClusterID == principal.ClusterID ||
		(w.ClusterID == "" && len(c.RegisteredClusters) == 1 && c.RegisteredClusters[0] != nil && c.RegisteredClusters[0].ClusterID == principal.ClusterID)
}

func workloadBurstBindingMatches(w *Workload, b *Burst, clusterID string) bool {
	return b.ID == w.BurstID && b.CustomerID == w.CustomerID &&
		(w.ClusterID == "" || b.ClusterID == "" || w.ClusterID == b.ClusterID) &&
		(clusterID == "" || b.ClusterID == "" || clusterID == b.ClusterID)
}
