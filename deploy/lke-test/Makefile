# Throwaway LKE cluster to verify the LKE providerID fix (burst nodes survive
# LKE's CCM). LKE control plane is FREE; 1x g6-standard-1 ≈ $0.018/hr. A full
# verify run is pennies. Prefer `make verify` — teardown is guaranteed.
#
# Prereqs: LINODE_TOKEN, jq, openssl, helm, kubectl.

CLUSTER   ?= yscale-lke-test
REGION    ?= us-ord
NODE_TYPE ?= g6-standard-1
KCFG      := $(CURDIR)/kubeconfig.yaml
CHART     := ../helm/yscale-agent

YSCALE_TOKEN ?=
ENDPOINT     ?= wss://api.yscale.sh

# Every recipe that talks to the cluster uses the LKE kubeconfig.
export KUBECONFIG := $(KCFG)
export CLUSTER
export REGION
export NODE_TYPE

.PHONY: help verify up down kubeconfig status agent survive nuke cost

help:
	@echo "make verify CONFIRM_SPEND=yes YSCALE_TOKEN=... FABRIC_*=... - survival + zero-residue proof"
	@echo "make up        - create the LKE cluster + write kubeconfig"
	@echo "make down      - delete the cluster"
	@echo "make kubeconfig- print the KUBECONFIG export line"
	@echo "make status    - show cluster + nodes"
	@echo "make agent YSCALE_TOKEN=... [ENDPOINT=...] - install yscale-agent"
	@echo "make survive [BACKEND=linode] - burst a node, assert it survives LKE's CCM"
	@echo "make nuke      - delete ANY lingering yscale-lke-test* cluster (orphan safety)"

verify:
	CLUSTER="$(CLUSTER)-verify-$$(date +%s)-$$PPID" ./verify.sh

up:
	./lke.sh up

down:
	./lke.sh delete

kubeconfig:
	@echo "export KUBECONFIG=$(KCFG)"

status:
	./lke.sh status

agent:
	@test -n "$(YSCALE_TOKEN)" || { echo "set YSCALE_TOKEN=yscale_..."; exit 1; }
	helm upgrade --install yscale-agent $(CHART) \
	  --namespace yscale --create-namespace \
	  --set token=$(YSCALE_TOKEN) \
	  --set endpoint=$(ENDPOINT) \
	  --set cloudProvider=linode
	kubectl -n yscale rollout status deploy/yscale-agent --timeout=180s

# Reuses the cluster-agnostic survival probe from ../eks-test (uses $KUBECONFIG).
survive:
	../eks-test/survive-test.sh

nuke:
	./lke.sh nuke

cost:
	@echo "LKE control plane: FREE.  1x g6-standard-1 node: ~\$$0.018/hr."
	@echo "A full verify run (cluster ~10 min + a transient burst): a few cents."
	@echo "Left running: ~\$$0.018/hr. Still: ALWAYS 'make down'."
