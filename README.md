# rhoai-crd-compare

Compare operator and CRD versions for Red Hat OpenShift AI, Red Hat Connectivity Link, and OpenShift Service Mesh (plus the Gateway API CRDs they share) across two clusters. The clusters do not need to be reachable at the same time: you snapshot each one when you have access, then compare the files offline.

## Layout

| Path | Purpose |
|---|---|
| `crdcompare.py` | The tool. Python 3.8+ standard library only. |
| `collect.sh` | Bash collector for hosts without Python. Uses only `oc get`. |
| `products.example.json` | Example of adding a product (Kueue) to the default set. |
| `snapshots/` | Put `*.json.gz` snapshots here. |
| `raw/` | Put `*.tar.gz` exports from `collect.sh` here. |
| `reports/` | Comparison reports. |

Snapshots contain cluster metadata, so `.gitignore` keeps `snapshots/`, `raw/`, and `reports/` out of Git.

## Requirements

- Python 3.8 or later (macOS: `python3 --version`)
- `oc` (or `kubectl` with `--cli kubectl`), logged in to the cluster with read access to CRDs, Subscriptions, and CSVs

The tool is read-only. It only runs `get` and `config view`.

## 1. Take a snapshot of each cluster

Log in to cluster A and run:

```bash
./crdcompare.py snapshot --label prod -o snapshots/prod.json.gz
```

Later, log in to cluster B (or point to its kubeconfig or context) and run:

```bash
./crdcompare.py snapshot --label dr --context dr-admin -o snapshots/dr.json.gz
```

Confirm the snapshot picked up what you expect:

```bash
./crdcompare.py show snapshots/prod.json.gz
```

### Customer jump host without Python

Copy `collect.sh` to the jump host and run it there:

```bash
bash collect.sh prod        # writes prod.tar.gz
```

Bring `prod.tar.gz` back into `raw/` and build the snapshot locally:

```bash
./crdcompare.py snapshot --from-dir raw/prod.tar.gz --label prod -o snapshots/prod.json.gz
```

If you change products with `--products-file` or `--product`, regenerate the collector with `./crdcompare.py print-commands --products-file ... > collect.sh`.

## 2. Compare

```bash
./crdcompare.py compare snapshots/prod.json.gz snapshots/dr.json.gz
./crdcompare.py compare snapshots/prod.json.gz snapshots/dr.json.gz --format markdown -o reports/prod-vs-dr.md
./crdcompare.py compare snapshots/prod.json.gz snapshots/dr.json.gz --format json -o reports/prod-vs-dr.json
```

The report covers, per product:

- **Operators**: CSV version, channel, catalog source, approval mode, CSV phase
- **CRDs**: defined, served, and storage versions, `status.storedVersions`, conversion strategy, OLM owner, bundle-version annotations, and a schema diff for each common version (added, removed, and retyped fields)
- **Instances**: DSC/DSCI release version, Istio/IstioRevision/IstioCNI version, Kuadrant/Authorino/Limitador readiness, GatewayClass controller

## Useful options

| Option | Command | Effect |
|---|---|---|
| `--all` | compare | Also list identical operators and CRDs |
| `--product rhoai,service-mesh` | snapshot, compare | Limit scope |
| `--fields N` | compare | Max field changes listed per CRD version (default 15, `0` hides) |
| `--exit-code` | compare | Exit 1 when differences are found (CI use) |
| `--redact` | snapshot | Omit API server URL and cluster ID |
| `--no-fields` | snapshot | Store schema hashes only; smaller file, no field-level diff |
| `--products-file FILE` | snapshot, print-commands | Add or override product definitions |

Product keys: `rhoai`, `connectivity-link`, `service-mesh`, `gateway-api`.

## Adding products

`products.example.json` adds Kueue. Entries in the file are merged with the defaults by key, so reusing an existing key (for example `rhoai`) replaces that product's definition.

```bash
./crdcompare.py snapshot --products-file products.example.json --label prod -o snapshots/prod.json.gz
```

Take both snapshots with the same product definitions. If the two differ, `compare` prints a warning, because some differences may come from classification rather than from the clusters.

## Notes

- CRDs are classified by API group and operators by OLM package name. Run `show` on the first real snapshot of each environment to confirm nothing is missing.
- A schema change with the same field list (`validation/description only`) usually means updated enums, defaults, or descriptions between operator builds.
- Snapshots are gzip-compressed JSON, so you can inspect them with `gunzip -c file.json.gz | jq`.
