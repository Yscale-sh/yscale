package state

// Callers must hold the account/membership serialization boundary through the
// workspace commit. The returned account from an earlier sign-in is not an
// eligibility decision for a later write.
func firstWorkspaceEligibility(owner *Account, hasTenant bool) error {
	if owner == nil {
		return ErrNotFound
	}
	if !owner.EmailVerified {
		return ErrAccountEmailUnverified
	}
	if hasTenant {
		return ErrAccountHasTenant
	}
	return nil
}
