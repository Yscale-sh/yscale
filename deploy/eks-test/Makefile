# Ephemeral EKS test cluster for yscale managed-cluster validation.
# Cost ≈ $0.15 per short test (cluster only; burst VM time additional).
# The real risk is forgetting teardown (~$0.15/hr ≈ $110/mo if left running).
# Prefer `make test` — it guarantees teardown via a trap. See README.md.

CLUSTER ?= yscale-eks-test
REGION  ?= us-east-1
CFG     := cluster.yaml
CHART   := ../helm/yscale-agent
RUN_ID  ?= eks-$(shell date +%s)-$(shell echo $$$$)

# yscale-agent install params (override on the CLI):
#   make agent YSCALE_TOKEN=yscale_... ENDPOINT=wss://api.yscale.sh
YSCALE_TOKEN ?=
ENDPOINT     ?= wss://api.yscale.sh

# Safety: default behavior does NOT spend money.
CONFIRM_SPEND ?=
MAX_TEST_SECONDS ?= 2700
BACKEND ?= linode

# Burst VM sweep — set from the Workload's .status.burstID, not from RUN_ID.
BURST_ID ?=

export CONFIRM_SPEND
export RUN_ID
export MAX_TEST_SECONDS
export BACKEND

.PHONY: help preflight up down kubeconfig status agent test survive watch-nodes nuke orphan-sweep cost

help:
	@echo "make preflight - validate tools and credentials (no cloud writes)"
	@echo "make test CONFIRM_SPEND=yes - up -> validate -> ALWAYS down (guaranteed teardown; safest)"
	@echo "make up CONFIRM_SPEND=yes   - create the cluster (REMEMBER: make down when finished)"
	@echo "make down      - delete the cluster"
	@echo "make kubeconfig- point kubectl at the cluster"
	@echo "make status    - show cluster + nodes"
	@echo "make agent YSCALE_TOKEN=... [ENDPOINT=...] - install yscale-agent"
	@echo "make survive [BACKEND=linode|aws] - burst a node, assert it survives the CCM"
	@echo "make watch-nodes - live-watch for the CCM reaping burst nodes"
	@echo "make orphan-sweep BURST_ID=... - check for orphan EC2 instances from a burst"
	@echo "make nuke      - delete ANY lingering yscale-eks-test* cluster (orphan safety)"
	@echo "make cost      - print the cost breakdown"

preflight:
	@command -v eksctl >/dev/null || { echo "FAIL: eksctl not installed: brew install eksctl"; exit 1; }
	@command -v aws    >/dev/null || { echo "FAIL: aws cli not installed"; exit 1; }
	@command -v kubectl >/dev/null || { echo "FAIL: kubectl not installed"; exit 1; }
	@command -v helm   >/dev/null || { echo "FAIL: helm not installed"; exit 1; }
	@aws sts get-caller-identity >/dev/null || { echo "FAIL: AWS creds not configured (aws configure / env)"; exit 1; }
	@echo "OK: all tools and credentials present"

up: preflight
	@test "$(CONFIRM_SPEND)" = "yes" || { echo "Set CONFIRM_SPEND=yes to create a PAID cluster (~\$$0.15/hr)."; exit 1; }
	eksctl create cluster -f $(CFG)
	@echo ""
	@echo ">>> Cluster is UP and billing (~\$$0.15/hr). Run 'make down' when finished. <<<"

down:
	eksctl delete cluster --name $(CLUSTER) --region $(REGION) --wait

kubeconfig:
	aws eks update-kubeconfig --name $(CLUSTER) --region $(REGION)

status:
	@eksctl get cluster --name $(CLUSTER) --region $(REGION) 2>/dev/null || echo "no cluster $(CLUSTER) in $(REGION)"
	@kubectl get nodes 2>/dev/null || true

agent:
	@test -n "$(YSCALE_TOKEN)" || { echo "set YSCALE_TOKEN=yscale_... (and optionally ENDPOINT=...)"; exit 1; }
	helm upgrade --install yscale-agent $(CHART) \
	  --namespace yscale --create-namespace \
	  --set token=$(YSCALE_TOKEN) \
	  --set endpoint=$(ENDPOINT) \
	  --set cloudProvider=aws
	kubectl -n yscale rollout status deploy/yscale-agent --timeout=180s

test:
	./ephemeral-test.sh

survive:
	./survive-test.sh

watch-nodes:
	./watch-burst-nodes.sh

orphan-sweep:
	@test -n "$(BURST_ID)" || { echo "Set BURST_ID= (from Workload .status.burstID)"; exit 1; }
	@echo "Checking for orphan EC2 instances (burst_id=$(BURST_ID))..."
	@orphans=$$(aws ec2 describe-instances \
	  --region $(REGION) \
	  --filters "Name=tag:yscale-owner,Values=burst" \
	            "Name=tag:yscale-burst-id,Values=$(BURST_ID)" \
	            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
	  --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || true); \
	if [ -n "$$orphans" ]; then \
	  echo "ORPHANS FOUND: $$orphans"; \
	  echo "  Verify and terminate manually: aws ec2 terminate-instances --instance-ids $$orphans --region $(REGION)"; \
	  exit 1; \
	else \
	  echo "OK: no orphan instances for burst $(BURST_ID)"; \
	fi

nuke:
	@for c in $$(eksctl get clusters --region $(REGION) -o json 2>/dev/null | jq -r '.[].Name' 2>/dev/null | grep '^yscale-eks-test' || true); do \
	  echo "deleting lingering cluster $$c"; \
	  eksctl delete cluster --name $$c --region $(REGION) --wait; \
	done; \
	echo "nuke sweep done"

cost:
	@echo "Per short test (~45 min cluster-up): ~\$$0.15 (cluster) + burst VM time"
	@echo "  EKS control plane  \$$0.10/hr (per hour)   -> ~\$$0.10"
	@echo "  1x t3.medium node  ~\$$0.042/hr (per sec)  -> ~\$$0.03"
	@echo "  Linode 4 GB burst     \$$0.036/hr          -> <= \$$0.036 for this test"
	@echo "  EKS Hybrid Nodes fee  \$$0.020/vCPU-hr     -> <= \$$0.04 for a 2-vCPU burst hour"
	@echo "  NAT gateway        \$$0.045/hr             -> \$$0 (disabled by design)"
	@echo "  EBS + public IPv4                          -> ~\$$0.01"
	@echo "If left running: ~\$$0.15/hr ~= \$$3.50/day ~= \$$110/mo. ALWAYS 'make down'."
