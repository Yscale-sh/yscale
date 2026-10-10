# Ephemeral GKE test cluster for yscale managed-cluster validation.
# Cost ≈ $0.10 per short test (cluster only; burst VM time additional).
# Prefer `make test` — teardown is guaranteed. See README.md.

CLUSTER  ?= yscale-gke-test
ZONE     ?= us-central1-a
REGION   ?= us-central1
PROJECT  ?=
CHART    := ../helm/yscale-agent
RUN_ID   ?= gke-$(shell date +%s)-$(shell echo $$$$)

YSCALE_TOKEN ?=
ENDPOINT     ?= wss://api.yscale.sh

CONFIRM_SPEND    ?=
MAX_TEST_SECONDS ?= 2700

# Burst VM sweep — set from the Workload's .status.burstID, not from RUN_ID.
BURST_ID ?=

export CLUSTER ZONE REGION PROJECT RUN_ID
export CONFIRM_SPEND MAX_TEST_SECONDS

.PHONY: help preflight up down kubeconfig status agent test survive nuke orphan-sweep cost

help:
	@echo "make preflight - validate tools and credentials (no cloud writes)"
	@echo "make test CONFIRM_SPEND=yes - up -> validate -> ALWAYS down (guaranteed teardown)"
	@echo "make up CONFIRM_SPEND=yes   - create the cluster"
	@echo "make down      - delete the cluster"
	@echo "make kubeconfig- point kubectl at the cluster"
	@echo "make status    - show cluster + nodes"
	@echo "make agent YSCALE_TOKEN=... [ENDPOINT=...] - install yscale-agent"
	@echo "make survive [BACKEND=gcp] - burst a node, assert it survives the CCM"
	@echo "make orphan-sweep BURST_ID=... - check for orphan GCE instances from a burst"
	@echo "make nuke      - delete ANY lingering yscale-gke-test* cluster"
	@echo "make cost      - print the cost breakdown"

preflight:
	./cluster.sh preflight

up: preflight
	@test "$(CONFIRM_SPEND)" = "yes" || { echo "Set CONFIRM_SPEND=yes to create a PAID cluster (~\$$0.10/hr)."; exit 1; }
	./cluster.sh up

down:
	./cluster.sh delete

kubeconfig:
	./cluster.sh kubeconfig

status:
	./cluster.sh status

agent:
	@test -n "$(YSCALE_TOKEN)" || { echo "set YSCALE_TOKEN=yscale_..."; exit 1; }
	helm upgrade --install yscale-agent $(CHART) \
	  --namespace yscale --create-namespace \
	  --set token=$(YSCALE_TOKEN) \
	  --set endpoint=$(ENDPOINT) \
	  --set cloudProvider=gcp
	kubectl -n yscale rollout status deploy/yscale-agent --timeout=180s

test:
	./ephemeral-test.sh

survive:
	../eks-test/survive-test.sh

orphan-sweep:
	@test -n "$(BURST_ID)" || { echo "Set BURST_ID= (from Workload .status.burstID)"; exit 1; }
	BURST_ID=$(BURST_ID) ./cluster.sh burst-check

nuke:
	./cluster.sh nuke

cost:
	@echo "Per short test (~30 min cluster-up): ~\$$0.10 (cluster) + burst VM time"
	@echo "  GKE control plane  \$$0.10/hr (Standard zonal)  -> ~\$$0.05"
	@echo "  1x e2-medium node  ~\$$0.034/hr                 -> ~\$$0.02"
	@echo "  Burst VM (e2-medium equiv) ~\$$0.034/hr         -> varies (budget.deadline)"
	@echo "  pd-standard 20GB   ~\$$0.0011/hr                -> ~\$$0.00"
	@echo "  NAT gateway        \$$0/hr                      -> \$$0 (not provisioned)"
	@echo "  Cloud Logging/Mon  \$$0/hr                      -> \$$0 (disabled by design)"
	@echo "If left running: ~\$$0.14/hr ~= \$$3.30/day ~= \$$100/mo. ALWAYS 'make down'."
