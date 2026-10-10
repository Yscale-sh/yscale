# Smoke-test workload: minimal busybox CPU pod that waits for the smoke
# driver to release it, then exits 0.
# Used by scripts/smoke-test.sh — placeholders substituted at apply time:
#   __TEST_ID__     — unique per smoke run; lets cleanup target only its
#                     own resources (per cleanup_safety_directive memory).
#   __SIZE__        — burst-template size alias (nano/small/medium/...).
#                     "nano" maps to 0.5 CPU / 0.5 GiB → cheapest path.
#   __BACKEND__     — explicit backend hint for the decider. Omit to let
#                     the decider auto-route (auto + no GPU → flyio).
#   __NAMESPACE__   — customer K8s namespace. Must be in the agent's
#                     rbac.allowedNamespaces.
apiVersion: batch/v1
kind: Job
metadata:
  name: yscale-smoke-__TEST_ID__
  namespace: __NAMESPACE__
  labels:
    yscale.sh/smoke-test-id: "__TEST_ID__"
    yscale.sh/smoke-test-backend: "__BACKEND__"
spec:
  ttlSecondsAfterFinished: 300   # auto-clean the Job 5min post-finish
  backoffLimit: 0                # one shot, no retries on failure
  template:
    metadata:
      labels:
        yscale.sh/smoke-test-id: "__TEST_ID__"
      annotations:
        # Marks the pod for the agent's pending-pod watcher. Without
        # this annotation, the pod stays Pending forever — the watcher
        # only acts on pods that explicitly request a burst.
        yscale.sh/burst-template: |
          size: __SIZE__
          backend: __BACKEND__
          budget:
            maxUSD: 0.05
            deadline: 10m
    spec:
      restartPolicy: Never
      # Routes the pod onto the (yet-to-be-provisioned) burst node.
      # Selector matches the label kubelet stamps via --node-labels.
      nodeSelector:
        yscale.sh/burst-node: "true"
      # Tolerations match the taints kubelet stamps via
      # --register-with-taints. Without these the scheduler skips the
      # burst even after it joins as Ready.
      tolerations:
        - key: yscale.sh/burst-node
          operator: Equal
          value: "true"
          effect: NoSchedule
        - key: node.cilium.io/agent-not-ready
          operator: Equal
          value: "true"
          effect: NoSchedule
      containers:
        - name: smoke
          image: busybox:1.36
          # Deterministic: prints expected MARKER on success so the
          # smoke-test script can grep for it in pod logs.
          command:
            - sh
            - -c
            - |
              echo "yscale-smoke: running on $(hostname)"
              # Keep the Job non-terminal while its exact provisioned burst
              # boots. The driver releases this only after exact-node
              # kubectl logs + exec probes pass.
              while [ ! -f /tmp/yscale-smoke-release ]; do sleep 1; done
              echo "yscale-smoke: MARKER-OK"
              # Do not race natural node teardown with the driver's final log
              # read. Completion is released only after that read succeeds.
              while [ ! -f /tmp/yscale-smoke-marker-read ]; do sleep 1; done
          resources:
            requests: { cpu: "50m",  memory: 64Mi }
            limits:   { cpu: "200m", memory: 128Mi }
