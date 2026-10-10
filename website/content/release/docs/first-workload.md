# Your first workload

For workload authors using a Yscale installation their operator has already
configured and verified. If you only ran the local fake-factory smoke, return
to [Getting started](getting-started.md): that smoke cannot execute a pod.

You need `kubectl`, access to the intended cluster and namespace, and a small
provider test approved by the person paying the bill. The examples below use
`default`; your operator must authorize the same namespace in central and the
connector's RBAC. Do not request the central admin token or provider keys.

## Prepare a small CPU job

From the source or release-bundle root:

```sh
kubectl config current-context
kubectl get crd workloads.yscale.sh
cp examples/workloads/hello.yaml first-burst.yaml
```

Review `first-burst.yaml` before applying it:

- Confirm `metadata.namespace` is your allowed namespace and choose a unique
  `metadata.name` if `hello` already exists.
- The example uses BusyBox, CPU size `nano`, no GPU and no storage integration.
  Your operator must confirm that size maps to available capacity on the
  selected backend. It is not a promise that every provider accepts it.
- Set `spec.budget.maxUSD` and `spec.budget.deadline` from the operator's cost
  estimate and expected boot time. The example's values are fixtures, not a
  quoted price or guaranteed ceiling; full-network startup can outlast a short
  deadline. Include provider teardown time and persistent mesh costs separately.

To inspect the local Job translation without contacting a cluster:

```sh
./bin/yscale apply --dry-run -f first-burst.yaml
```

This checks the local translation only, not provider compatibility or central
admission. **Submit the Workload with `kubectl` below.** `yscale apply` without
`--dry-run` creates a Job directly and is a different path; it does not create
the Workload custom resource used in this guide.

## Submit and inspect

The following command can provision paid cloud capacity:

```sh
kubectl apply -f first-burst.yaml
kubectl -n default get workloads.yscale.sh hello -w
```

In another terminal (adjust the name and namespace if you changed them):

```sh
kubectl -n default get jobs,pods -l yscale.sh/workload=hello -o wide
kubectl -n default describe workloads.yscale.sh hello
kubectl -n default logs -l yscale.sh/workload=hello --all-containers=true
```

The connector sends the request to central; central selects configured
capacity and starts a burst; a node enrolls; the Job runs on that node; then
completion drives cleanup. Expect provisioning and joining to take time. If a
stage fails, retain the Workload status and events before changing anything.

Successful application of YAML is not success of the job. Look for the
example's `[hello] done` output and successful pod completion. Ask the operator
to verify the burst node and provider resource are removed after cleanup.
Do not infer successful teardown solely from a terminal Workload phase.

## Cancel or finish the test

To cancel a running example, or remove its completed Workload record:

```sh
kubectl -n default delete workloads.yscale.sh hello
```

The connector requests central cancellation when the Workload is deleted.
Deletion is asynchronous; it is not a receipt proving the provider stopped
charging. Keep the connector and control plane running until the operator
confirms cleanup. Do not uninstall the chart first or strip finalizers to hide
unresolved cleanup.

Have the operator inspect remaining instances, disks, interfaces and mesh
membership. The tenant's persistent coordination box is separate from this
job and remains until tenant offboarding. Offboarding is an operator action
that revokes the tenant and may affect every workload belonging to it.

## Use Git, Flux or Argo afterward

Store the reviewed Workload manifest in your existing application repository;
deliver it to the allowed namespace through your normal deployment workflow.
Yscale uses a Kubernetes [custom resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/),
not a separate editor or source-control system.

For one-off jobs, use a new Workload name for a new run. Avoid having two tools
own the same object. If GitOps owns it, remove or suspend that desired state
before a manual cancellation, or the controller may recreate the resource.
That recreation is particularly important when the resource requests billable
capacity. Do not commit tokens or provider credentials with the workload.

Pending-pod examples for Deployments, KEDA and Argo Workflows are included under
[examples/workloads](../examples/workloads). They require separate operator
configuration and RBAC/idle-reaping review; they are not the first-job path
documented here.
