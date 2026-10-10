# Ephemeral AKS test cluster for yscale managed-cluster validation.
# Cost ≈ $0.28 per short test (Free tier control plane; includes a short burst).
# Prefer `make test` — teardown is guaranteed. See README.md.

CLUSTER        ?= yscale-aks-test
LOCATION       ?= eastus
RESOURCE_GROUP ?=
CHART          := ../helm/yscale-agent
RUN_ID         ?= aks-$(shell date +%s)-$(shell echo $$$$)

YSCALE_TOKEN ?=
ENDPOINT     ?= wss://api.yscale.sh

CONFIRM_SPEND    ?=
MAX_TEST_SECONDS ?= 2700
CENTRAL_AZURE_SCOPE_VERIFIED ?=

# Burst VM sweep — set from the Workload's .status.burstID, not from RUN_ID.
BURST_ID ?=

export CLUSTER LOCATION RESOURCE_GROUP RUN_ID
export CONFIRM_SPEND MAX_TEST_SECONDS
export CENTRAL_AZURE_SCOPE_VERIFIED

.PHONY: help preflight up down kubeconfig status agent test survive nuke orphan-sweep cost

help:
	@echo "make preflight - validate tools and credentials (no cloud writes)"
	@echo "make test CONFIRM_SPEND=yes - up -> validate -> ALWAYS down (guaranteed teardown)"
	@echo "make up CONFIRM_SPEND=yes   - create the cluster"
	@echo "make down      - delete the cluster"
	@echo "make kubeconfig- point kubectl at the cluster"
	@echo "make status    - show cluster + nodes"
	@echo "make agent YSCALE_TOKEN=... [ENDPOINT=...] - install yscale-agent"
	@echo "make survive [BACKEND=azure] - burst a node, assert it survives the CCM"
	@echo "make orphan-sweep BURST_ID=... - check for orphan Azure VMs from a burst"
	@echo "make nuke      - delete ANY lingering yscale-aks-test* cluster"
	@echo "make cost      - print the cost breakdown"

preflight:
	./cluster.sh preflight

up: preflight
	@test "$(CONFIRM_SPEND)" = "yes" || { echo "Set CONFIRM_SPEND=yes to create paid AKS resources (~\$$0.39/hr plus burst)."; exit 1; }
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
	  --set cloudProvider=azure
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
	@echo "Per short test (~30 min cluster-up): ~\$$0.20 (cluster) + ~\$$0.08 short burst"
	@echo "  AKS control plane  \$$0.00/hr (Free tier)       -> \$$0.00"
	@echo "  2x Standard_D4as_v5 ~\$$0.344/hr (4 vCPU each) -> ~\$$0.17"
	@echo "  Burst VM (Standard_D2s_v5 equiv) ~\$$0.096/hr   -> varies (budget.deadline)"
	@echo "  Target disks + outbound networking allowance    -> ~\$$0.03/hr"
	@echo "  Burst NAT + public IP ~\$$0.050/resource-hour    -> ~\$$0.05 minimum + data"
	@echo "If left running: cluster ~\$$0.39/hr; burst NAT/IP ~\$$0.05/hr. ALWAYS 'make down'."
