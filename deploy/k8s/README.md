# KayaDB Kubernetes deployment

Run a **3-node Raft cluster** as a StatefulSet with stable pod DNS and persistent volumes.

## Prerequisites

- Kubernetes 1.25+ (kind, minikube, or a cloud cluster)
- `kubectl` configured for your cluster
- Image `kayadb:latest` available to the cluster (build from `deploy/docker/Dockerfile`)

### Build and load the image (local clusters)

From the repository root:

```bash
docker build -f deploy/docker/Dockerfile -t kayadb:latest .
```

**kind:**

```bash
kind load docker-image kayadb:latest
```

**minikube:**

```bash
minikube image load kayadb:latest
```

## Apply order

Apply manifests in dependency order:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

Or apply the whole directory at once (Kubernetes resolves creation order within the same apply):

```bash
kubectl apply -f deploy/k8s/
```

## Verify the cluster

```bash
kubectl -n kayadb get pods -l app=kayadb -w
kubectl -n kayadb get pvc
kubectl -n kayadb get svc kayadb-headless
```

Expected pods: `kayadb-0`, `kayadb-1`, `kayadb-2`. Each pod gets a 10 Gi `ReadWriteOnce` PVC at `/data`.

### Peer DNS

Pods discover peers via the headless service:

| Node ID | Pod       | Raft DNS                         | Client DNS                         |
|---------|-----------|----------------------------------|------------------------------------|
| 1       | `kayadb-0` | `kayadb-0.kayadb-headless:7481` | `kayadb-0.kayadb-headless:7379` |
| 2       | `kayadb-1` | `kayadb-1.kayadb-headless:7481` | `kayadb-1.kayadb-headless:7379` |
| 3       | `kayadb-2` | `kayadb-2.kayadb-headless:7481` | `kayadb-2.kayadb-headless:7379` |

`configmap.yaml` documents the same roster as `KAYADB_PEER_NODE_*` hints. The `start.sh` entry derives `--node-id` from the pod ordinal and passes `--peer` flags automatically.

## Client access (port-forward)

Forward the client port on pod `kayadb-0` to your machine:

```bash
kubectl -n kayadb port-forward pod/kayadb-0 7379:7379
```

In another terminal (with a local `kayactl` binary or via `docker exec` against a loaded image):

```bash
kayactl --server 127.0.0.1:7379 put hello world
kayactl --server 127.0.0.1:7379 get hello
kayactl --server 127.0.0.1:7379 status
```

Or run `kayactl` inside the pod:

```bash
kubectl -n kayadb exec -it kayadb-0 -- \
  kayactl --server 127.0.0.1:7379 status
```

## Configuration notes

- **Image:** `kayadb:latest` (`imagePullPolicy: IfNotPresent` for local builds)
- **Ports:** `7379` (client), `7481` (raft) on every pod
- **Storage:** `volumeClaimTemplates` request 10 Gi per replica
- **Flags:** `--allow-public-bind` and `--no-metrics` match the Docker Compose demo layout

Optional production hardening (not included in this demo):

- `KAYA_OPERATOR_TOKEN` / `KAYA_CLIENT_TOKEN` for auth
- Native TLS (`--tls-cert`, `--tls-key`, `--tls-ca`) or an mTLS sidecar
- Resource requests/limits and PodDisruptionBudgets

## Teardown

```bash
kubectl delete -f deploy/k8s/
```

PVCs are retained by default after StatefulSet deletion. Remove them explicitly if you want a clean slate:

```bash
kubectl -n kayadb delete pvc -l app=kayadb
```