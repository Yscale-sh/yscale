package state

import (
	"context"
	"time"
)

func rejectSchedulingObservation(context.Context, string, string, string, string, string, string, string, time.Time) (ObservationResult, error) {
	return ObservationRejected, nil
}

func (p *accountSpyPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *burstFailPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *gatewayRouteSpyPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *podSlotPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *idemPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *provisioningPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *podObsFailPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *workloadPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *listOnlyPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}

func (p *claimPersister) stampWorkloadSchedulingObservation(ctx context.Context, workloadID, customerID, clusterID, state, reason, message, podName string, observedAt time.Time) (ObservationResult, error) {
	return rejectSchedulingObservation(ctx, workloadID, customerID, clusterID, state, reason, message, podName, observedAt)
}
