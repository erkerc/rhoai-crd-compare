#!/usr/bin/env python3
"""
crdcompare - snapshot and compare operator / CRD versions for
Red Hat OpenShift AI, Red Hat Connectivity Link and OpenShift Service Mesh
(plus the Gateway API CRDs all three depend on) across two clusters that
may never be reachable at the same time.

Workflow
  1. With access to cluster A:  ./crdcompare.py snapshot --label prod -o prod.json.gz
  2. Later, cluster B:          ./crdcompare.py snapshot --label dr   -o dr.json.gz
  3. Anywhere, offline:         ./crdcompare.py compare prod.json.gz dr.json.gz

No Python on the jump host? Collect raw JSON with plain oc and build the
snapshot later on your laptop:
     ./crdcompare.py print-commands > collect.sh      # copy + run where oc is logged in
     bash collect.sh prod                              # -> prod.tar.gz
     ./crdcompare.py snapshot --from-dir prod.tar.gz --label prod -o prod.json.gz

Requirements: Python 3.8+ (stdlib only) and oc or kubectl for live snapshots.
The tool is read-only: it only runs `get` and `config view`.
"""

import argparse
import copy
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter

TOOL_VERSION = "1.0.0"
SNAPSHOT_FORMAT = 1

CRD_RES = "customresourcedefinitions.apiextensions.k8s.io"
CSV_RES = "clusterserviceversions.operators.coreos.com"
SUB_RES = "subscriptions.operators.coreos.com"
CV_RES = "clusterversions.config.openshift.io"

# Product definitions. Regexes match OLM package names and CRD API groups.
# Override or extend with --products-file (same structure, merged per product key).
DEFAULT_PRODUCTS = {
    "rhoai": {
        "title": "Red Hat OpenShift AI",
        "packages": [r"^rhods-operator$", r"^opendatahub-operator$"],
        "crd_groups": [
            r"(^|\.)opendatahub\.io$",
            r"(^|\.)kserve\.io$",
            r"(^|\.)kubeflow\.org$",
            r"^ray\.io$",
            r"(^|\.)codeflare\.dev$",
            r"^llamastack\.io$",
            r"^feast\.dev$",
            r"^inference\.networking\.(x-)?k8s\.io$",
        ],
        "instances": [
            {"resource": "datascienceclusters.datasciencecluster.opendatahub.io",
             "fields": ["status.release.name", "status.release.version", "status.phase"]},
            {"resource": "dscinitializations.dscinitialization.opendatahub.io",
             "fields": ["status.release.version", "status.phase"]},
        ],
    },
    "connectivity-link": {
        "title": "Red Hat Connectivity Link",
        "packages": [r"^rhcl-operator$", r"^kuadrant-operator$", r"^authorino-operator$",
                     r"^limitador-operator$", r"^dns-operator$"],
        "crd_groups": [r"(^|\.)kuadrant\.io$"],
        "instances": [
            {"resource": "kuadrants.kuadrant.io", "fields": ["status.conditions[Ready]"]},
            {"resource": "authorinos.operator.authorino.kuadrant.io", "fields": ["status.conditions[Ready]"]},
            {"resource": "limitadors.limitador.kuadrant.io", "fields": ["status.conditions[Ready]"]},
        ],
    },
    "service-mesh": {
        "title": "OpenShift Service Mesh",
        "packages": [r"^servicemeshoperator3?$", r"^sailoperator$"],
        "crd_groups": [r"^sailoperator\.io$", r"(^|\.)istio\.io$", r"(^|\.)maistra\.io$"],
        "instances": [
            {"resource": "istios.sailoperator.io",
             "fields": ["spec.version", "status.state", "status.activeRevisionName"]},
            {"resource": "istiorevisions.sailoperator.io",
             "fields": ["spec.version", "status.conditions[Ready]"]},
            {"resource": "istiocnis.sailoperator.io", "fields": ["spec.version", "status.state"]},
            {"resource": "servicemeshcontrolplanes.maistra.io",
             "fields": ["spec.version", "status.chartVersion"]},
        ],
    },
    "gateway-api": {
        "title": "Gateway API (shared dependency)",
        "packages": [],
        "crd_groups": [r"^gateway\.networking\.(x-)?k8s\.io$"],
        "instances": [
            {"resource": "gatewayclasses.gateway.networking.k8s.io",
             "fields": ["spec.controllerName", "status.conditions[Accepted]"]},
        ],
    },
}

KEEP_LABELS = ("app.kubernetes.io/version", "app.kubernetes.io/part-of",
               "app.kubernetes.io/managed-by", "olm.managed")


def keep_annotation(key):
    return (key == "controller-gen.kubebuilder.io/version"
            or key.endswith("/bundle-version") or key.endswith("/channel"))


def eprint(*a):
    print(*a, file=sys.stderr)


def safe_name(resource):
    return re.sub(r"[^A-Za-z0-9._-]", "_", resource)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- products

class Products:
    def __init__(self, defs):
        self.defs = defs
        self._pkg = [(n, re.compile(rx)) for n, d in defs.items() for rx in d.get("packages", [])]
        self._grp = [(n, re.compile(rx)) for n, d in defs.items() for rx in d.get("crd_groups", [])]

    def by_package(self, pkg):
        return next((n for n, rx in self._pkg if pkg and rx.search(pkg)), None)

    def by_group(self, group):
        return next((n for n, rx in self._grp if group and rx.search(group)), None)

    def probes(self):
        for n, d in self.defs.items():
            for p in d.get("instances", []):
                yield n, p


def load_products(path, selected):
    defs = copy.deepcopy(DEFAULT_PRODUCTS)
    if path:
        with open(path) as fh:
            defs.update(json.load(fh))
    if selected:
        wanted = [s.strip() for s in selected.split(",") if s.strip()]
        unknown = [w for w in wanted if w not in defs]
        if unknown:
            raise SystemExit(f"unknown product(s): {', '.join(unknown)}; known: {', '.join(defs)}")
        defs = {k: defs[k] for k in wanted}
    return defs


# --------------------------------------------------------------------------- sources

class SourceError(Exception):
    pass


class NotFound(SourceError):
    pass


class LiveSource:
    kind = "live"

    def __init__(self, cli, kubeconfig, context, timeout):
        self.cli = cli
        self.base = [cli]
        if kubeconfig:
            self.base += ["--kubeconfig", kubeconfig]
        if context:
            self.base += ["--context", context]
        self.timeout = timeout

    def _run(self, args):
        try:
            p = subprocess.run(self.base + args, capture_output=True, text=True, timeout=self.timeout)
        except FileNotFoundError:
            raise SourceError(f"'{self.cli}' not found in PATH")
        except subprocess.TimeoutExpired:
            raise SourceError(f"timed out after {self.timeout}s: {self.cli} {' '.join(args)}")
        if p.returncode != 0:
            err = p.stderr.strip()
            if "doesn't have a resource type" in err or "NotFound" in err:
                raise NotFound(err)
            raise SourceError(err or f"exit code {p.returncode}")
        return p.stdout

    def get_json(self, resource):
        out = self._run(["get", resource, "-A", "-o", "json"])
        try:
            return json.loads(out)
        except ValueError as e:
            raise SourceError(f"invalid JSON from {resource}: {e}")

    def server(self):
        try:
            return self._run(["config", "view", "--minify", "-o",
                              "jsonpath={.clusters[0].cluster.server}"]).strip() or None
        except SourceError:
            return None


class DirSource:
    kind = "dir"

    def __init__(self, path):
        self.path = path

    def get_json(self, resource):
        f = os.path.join(self.path, safe_name(resource) + ".json")
        if not os.path.exists(f) or os.path.getsize(f) == 0:
            raise NotFound(f"{os.path.basename(f)} not in collected data")
        with open(f) as fh:
            try:
                return json.load(fh)
            except ValueError as e:
                raise SourceError(f"{os.path.basename(f)}: invalid JSON ({e})")

    def server(self):
        f = os.path.join(self.path, "server.txt")
        if os.path.exists(f):
            with open(f) as fh:
                return fh.read().strip() or None
        return None


def open_dir_source(path):
    if os.path.isdir(path):
        root = path
    elif os.path.isfile(path) and tarfile.is_tarfile(path):
        root = tempfile.mkdtemp(prefix="crdcompare-")
        with tarfile.open(path) as t:
            if hasattr(tarfile, "data_filter"):
                t.extractall(root, filter="data")
            else:
                t.extractall(root)
    else:
        raise SystemExit(f"--from-dir: {path} is neither a directory nor a tar archive")
    target = safe_name(CRD_RES) + ".json"
    for d, _, files in os.walk(root):
        if target in files:
            return DirSource(d)
    raise SystemExit(f"{target} not found under {path}; was it collected with print-commands?")


# --------------------------------------------------------------------------- snapshot

def schema_sha(schema):
    raw = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def flatten_schema(schema, prefix="", out=None, depth=0):
    """Map every property path in an OpenAPI v3 schema to its type."""
    if out is None:
        out = {}
    if not isinstance(schema, dict) or depth > 40:
        return out
    t = schema.get("type")
    if not t:
        if schema.get("x-kubernetes-int-or-string"):
            t = "int-or-string"
        elif schema.get("x-kubernetes-preserve-unknown-fields"):
            t = "any"
        else:
            t = "untyped"
    if prefix:
        out[prefix] = t
    for k, v in (schema.get("properties") or {}).items():
        flatten_schema(v, f"{prefix}.{k}" if prefix else k, out, depth + 1)
    if isinstance(schema.get("items"), dict):
        flatten_schema(schema["items"], prefix + "[]", out, depth + 1)
    if isinstance(schema.get("additionalProperties"), dict):
        flatten_schema(schema["additionalProperties"], prefix + "{}", out, depth + 1)
    return out


def get_path(obj, path):
    """Dotted path lookup; `conditions[Ready]` returns that condition's status."""
    cur = obj
    for part in path.split("."):
        m = re.fullmatch(r"([\w-]+)\[([\w-]+)\]", part)
        if m:
            lst = cur.get(m.group(1)) if isinstance(cur, dict) else None
            if not isinstance(lst, list):
                return None
            cond = next((c for c in lst if isinstance(c, dict) and c.get("type") == m.group(2)), None)
            if cond is None:
                return None
            st = cond.get("status")
            return st if st == "True" else f"{st} ({cond.get('reason', '')})"
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def csv_package(csv):
    md = csv.get("metadata", {})
    props = (md.get("annotations") or {}).get("operatorframework.io/properties")
    if props:
        try:
            for p in json.loads(props).get("properties", []):
                if p.get("type") == "olm.package":
                    return p.get("value", {}).get("packageName")
        except (ValueError, AttributeError):
            pass
    ns = md.get("namespace", "")
    for k in (md.get("labels") or {}):
        if k.startswith("operators.coreos.com/"):
            rest = k.split("/", 1)[1]
            if ns and rest.endswith("." + ns):
                return rest[: -(len(ns) + 1)]
    return re.sub(r"\.v?\d.*$", "", md.get("name", ""))


def make_operator(product, pkg, ns, sub, csv):
    entry = {"product": product, "package": pkg, "namespace": ns, "subscription": None, "csv": None}
    if sub:
        s, st = sub.get("spec", {}), sub.get("status", {})
        entry["subscription"] = {
            "name": sub["metadata"].get("name"), "channel": s.get("channel"), "source": s.get("source"),
            "approval": s.get("installPlanApproval"), "installed_csv": st.get("installedCSV"),
            "current_csv": st.get("currentCSV"), "state": st.get("state"),
        }
    if csv:
        s, st = csv.get("spec", {}), csv.get("status", {})
        entry["csv"] = {
            "name": csv["metadata"].get("name"), "version": s.get("version"),
            "replaces": s.get("replaces"), "phase": st.get("phase"), "display_name": s.get("displayName"),
        }
    return entry


def take_snapshot(src, products, label, with_fields, redact):
    errors = []

    def fetch(resource, fatal=False, quiet_missing=True):
        try:
            return src.get_json(resource).get("items", []) or []
        except NotFound as e:
            if fatal:
                raise SystemExit(f"cannot read {resource}: {e}")
            if not quiet_missing:
                errors.append({"resource": resource, "error": str(e)[:500]})
        except SourceError as e:
            if fatal:
                raise SystemExit(f"cannot read {resource}: {e}")
            errors.append({"resource": resource, "error": str(e)[:500]})
        return []

    eprint("reading CRDs ...")
    crds = []
    for c in fetch(CRD_RES, fatal=True):
        spec, md = c.get("spec", {}), c.get("metadata", {})
        group = spec.get("group", "")
        prod = products.by_group(group)
        if not prod:
            continue
        labels, ann = md.get("labels") or {}, md.get("annotations") or {}
        versions = []
        for v in spec.get("versions") or []:
            schema = (v.get("schema") or {}).get("openAPIV3Schema")
            ve = {"name": v.get("name"), "served": bool(v.get("served")),
                  "storage": bool(v.get("storage")), "deprecated": bool(v.get("deprecated", False)),
                  "schema_sha": schema_sha(schema) if schema is not None else None}
            if with_fields and schema is not None:
                ve["fields"] = flatten_schema(schema)
            versions.append(ve)
        crds.append({
            "product": prod, "name": md.get("name"), "group": group,
            "kind": spec.get("names", {}).get("kind"), "scope": spec.get("scope"),
            "conversion": (spec.get("conversion") or {}).get("strategy", "None"),
            "versions": versions,
            "stored_versions": sorted((c.get("status") or {}).get("storedVersions") or []),
            "olm_owners": sorted(k.split("/", 1)[1] for k in labels if k.startswith("operators.coreos.com/")),
            "annotations": {k: ann[k] for k in sorted(ann) if keep_annotation(k)},
            "labels": {k: labels[k] for k in KEEP_LABELS if k in labels},
        })
    crds.sort(key=lambda x: (x["product"], x["name"]))

    eprint("reading Subscriptions and ClusterServiceVersions ...")
    subs = fetch(SUB_RES, quiet_missing=False)
    csv_idx = {}
    for c in fetch(CSV_RES, quiet_missing=False):
        md = c.get("metadata", {})
        if "olm.copiedFrom" in (md.get("labels") or {}):
            continue
        csv_idx[(md.get("namespace"), md.get("name"))] = c
    operators, used = [], set()
    for s in subs:
        pkg = s.get("spec", {}).get("name")
        prod = products.by_package(pkg)
        if not prod:
            continue
        ns = s["metadata"].get("namespace")
        key = (ns, (s.get("status") or {}).get("installedCSV"))
        csv = csv_idx.get(key)
        if csv:
            used.add(key)
        operators.append(make_operator(prod, pkg, ns, s, csv))
    for key, c in csv_idx.items():
        if key in used:
            continue
        pkg = csv_package(c)
        prod = products.by_package(pkg)
        if prod:
            operators.append(make_operator(prod, pkg, key[0], None, c))
    operators.sort(key=lambda o: (o["product"], o["package"], o["namespace"] or ""))

    eprint("reading product instances ...")
    crd_names = {c["name"] for c in crds}
    instances = []
    for prod, probe in products.probes():
        if probe["resource"] not in crd_names:
            continue
        for item in fetch(probe["resource"]):
            md = item.get("metadata", {})
            instances.append({
                "product": prod, "resource": probe["resource"],
                "namespace": md.get("namespace"), "name": md.get("name"),
                "fields": {f: get_path(item, f) for f in probe["fields"]},
            })

    cluster = {"server": src.server()}
    cv = fetch(CV_RES)
    if cv:
        c = cv[0]
        cluster.update({
            "ocp_version": (c.get("status") or {}).get("desired", {}).get("version"),
            "ocp_channel": (c.get("spec") or {}).get("channel"),
            "cluster_id": (c.get("spec") or {}).get("clusterID"),
        })
    if redact:
        cluster["server"] = None
        cluster.pop("cluster_id", None)
    if not label:
        m = re.match(r"https?://api\.([^.:/]+)", cluster.get("server") or "")
        label = m.group(1) if m else "cluster"

    return {
        "snapshot_format": SNAPSHOT_FORMAT, "tool_version": TOOL_VERSION,
        "label": label, "taken_at": now_iso(), "source": src.kind,
        "cluster": cluster, "products": products.defs,
        "operators": operators, "crds": crds, "instances": instances, "errors": errors,
    }


def write_json(obj, path):
    data = json.dumps(obj, indent=1, sort_keys=False).encode()
    if path == "-":
        sys.stdout.buffer.write(data)
        return
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "wb") as fh:
        fh.write(data)


def load_snapshot(path):
    with open(path, "rb") as fh:
        head = fh.read(2)
    opener = gzip.open if head == b"\x1f\x8b" else open
    with opener(path, "rb") as fh:
        snap = json.loads(fh.read().decode())
    if "crds" not in snap or "operators" not in snap:
        raise SystemExit(f"{path} does not look like a crdcompare snapshot")
    return snap


# --------------------------------------------------------------------------- compare

def op_view(o):
    s, c = o.get("subscription") or {}, o.get("csv") or {}
    return {"namespace": o.get("namespace"), "csv": c.get("name"), "version": c.get("version"),
            "phase": c.get("phase"), "channel": s.get("channel"), "source": s.get("source"),
            "approval": s.get("approval"), "installed_csv": s.get("installed_csv")}


def compare_operators(ops_a, ops_b):
    multi = set()
    for ops in (ops_a, ops_b):
        multi |= {p for p, n in Counter(o["package"] for o in ops).items() if n > 1}

    def key(o):
        return f"{o['package']} ({o['namespace']})" if o["package"] in multi else o["package"]

    ia, ib = {key(o): o for o in ops_a}, {key(o): o for o in ops_b}
    rows = []
    for k in sorted(set(ia) | set(ib)):
        va = op_view(ia[k]) if k in ia else None
        vb = op_view(ib[k]) if k in ib else None
        if va and vb:
            diff = [f for f in va if va[f] != vb[f]]
            status = "changed" if diff else "same"
            # version/channel already appear in the table; CSV names only add noise when they track the version
            noise = {"version", "channel"} | ({"csv", "installed_csv"} if "version" in diff else set())
            changes = [f"{f}: {va[f]} -> {vb[f]}" for f in diff if f not in noise]
        else:
            changes, status = [], "only_a" if va else "only_b"
        rows.append({"key": k, "status": status, "a": va, "b": vb, "changes": changes})
    return rows


def crd_view(c):
    if not c:
        return None
    return {"storage": next((v["name"] for v in c["versions"] if v["storage"]), None),
            "served": [v["name"] for v in c["versions"] if v["served"]],
            "olm_owners": c.get("olm_owners", [])}


def diff_crd(x, y):
    changes, schema = [], []
    vx = {v["name"]: v for v in x["versions"]}
    vy = {v["name"]: v for v in y["versions"]}

    def j(items):
        return ",".join(items) or "-"

    if set(vx) != set(vy):
        changes.append(f"defined versions: {j(sorted(vx))} -> {j(sorted(vy))}")
    sx = sorted(n for n, v in vx.items() if v["served"])
    sy = sorted(n for n, v in vy.items() if v["served"])
    if sx != sy:
        changes.append(f"served versions: {j(sx)} -> {j(sy)}")
    stx, sty = crd_view(x)["storage"], crd_view(y)["storage"]
    if stx != sty:
        changes.append(f"storage version: {stx} -> {sty}")
    if x.get("stored_versions") != y.get("stored_versions"):
        changes.append(f"status.storedVersions: {j(x.get('stored_versions', []))} -> "
                       f"{j(y.get('stored_versions', []))}")
    if x.get("conversion") != y.get("conversion"):
        changes.append(f"conversion strategy: {x.get('conversion')} -> {y.get('conversion')}")
    for name in ("annotations", "labels"):
        ax, ay = x.get(name, {}), y.get(name, {})
        for k in sorted(set(ax) | set(ay)):
            if ax.get(k) != ay.get(k):
                changes.append(f"{k}: {ax.get(k)} -> {ay.get(k)}")
    if x.get("olm_owners") != y.get("olm_owners"):
        changes.append(f"OLM owner: {j(x.get('olm_owners', []))} -> {j(y.get('olm_owners', []))}")
    for n in sorted(set(vx) & set(vy)):
        a, b = vx[n], vy[n]
        if a["deprecated"] != b["deprecated"]:
            changes.append(f"{n} deprecated: {a['deprecated']} -> {b['deprecated']}")
        if a.get("schema_sha") == b.get("schema_sha"):
            continue
        fa, fb = a.get("fields"), b.get("fields")
        if fa is None or fb is None:
            changes.append(f"schema {n}: changed (field list not captured)")
            continue
        added = sorted(set(fb) - set(fa))
        removed = sorted(set(fa) - set(fb))
        retyped = [[k, fa[k], fb[k]] for k in sorted(set(fa) & set(fb)) if fa[k] != fb[k]]
        if added or removed or retyped:
            changes.append(f"schema {n}: +{len(added)} / -{len(removed)} fields, {len(retyped)} type changes")
        else:
            changes.append(f"schema {n}: changed (validation/description only, same fields)")
        schema.append({"version": n, "added": added, "removed": removed, "retyped": retyped})
    return changes, schema


def compare_crds(crds_a, crds_b):
    ia, ib = {c["name"]: c for c in crds_a}, {c["name"]: c for c in crds_b}
    rows = []
    for n in sorted(set(ia) | set(ib)):
        x, y = ia.get(n), ib.get(n)
        changes, schema = (diff_crd(x, y) if x and y else ([], []))
        status = ("changed" if changes else "same") if x and y else ("only_a" if x else "only_b")
        rows.append({"name": n, "status": status, "a": crd_view(x), "b": crd_view(y),
                     "changes": changes, "schema": schema})
    return rows


def compare_instances(ins_a, ins_b):
    def key(i):
        return f"{i['resource'].split('.')[0]} {i['namespace'] + '/' if i['namespace'] else ''}{i['name']}"

    ia, ib = {key(i): i for i in ins_a}, {key(i): i for i in ins_b}
    rows = []
    for k in sorted(set(ia) | set(ib)):
        fa = ia[k]["fields"] if k in ia else None
        fb = ib[k]["fields"] if k in ib else None
        if fa is not None and fb is not None:
            changes = [f"{f}: {fa.get(f)} -> {fb.get(f)}" for f in sorted(set(fa) | set(fb))
                       if fa.get(f) != fb.get(f)]
            status = "changed" if changes else "same"
        else:
            changes, status = [], "only_a" if fa is not None else "only_b"
        rows.append({"key": k, "status": status, "a": fa, "b": fb, "changes": changes})
    return rows


def snap_meta(s):
    c = s.get("cluster", {})
    return {"label": s.get("label"), "server": c.get("server"), "ocp_version": c.get("ocp_version"),
            "taken_at": s.get("taken_at"), "source": s.get("source"), "tool_version": s.get("tool_version")}


def compare_snapshots(a, b, product_filter):
    warnings = []
    for tag, s in (("A", a), ("B", b)):
        if s.get("snapshot_format") != SNAPSHOT_FORMAT:
            warnings.append(f"{tag} uses snapshot format {s.get('snapshot_format')}, expected {SNAPSHOT_FORMAT}")
        for e in s.get("errors", []):
            warnings.append(f"{tag} ({s.get('label')}) could not read {e['resource']}: {e['error']}")
    pa, pb = a.get("products", {}), b.get("products", {})
    names = list(dict.fromkeys(list(pa) + list(pb)))
    if product_filter:
        names = [n for n in names if n in product_filter]
    for n in names:
        if n in pa and n in pb:
            if pa[n] != pb[n]:
                warnings.append(f"product '{n}' was defined differently when each snapshot was taken; "
                                "some differences may come from classification, not the cluster")
        else:
            warnings.append(f"product '{n}' is only defined in snapshot {'A' if n in pa else 'B'}")
    report = {"a": snap_meta(a), "b": snap_meta(b), "warnings": warnings, "products": {}}
    for n in names:
        def pick(s, sec):
            return [x for x in s.get(sec, []) if x["product"] == n]
        report["products"][n] = {
            "title": (pa.get(n) or pb.get(n) or {}).get("title", n),
            "operators": compare_operators(pick(a, "operators"), pick(b, "operators")),
            "crds": compare_crds(pick(a, "crds"), pick(b, "crds")),
            "instances": compare_instances(pick(a, "instances"), pick(b, "instances")),
        }
    report["has_differences"] = any(r["status"] != "same" for p in report["products"].values()
                                    for sec in ("operators", "crds", "instances") for r in p[sec])
    return report


# --------------------------------------------------------------------------- rendering

def table(headers, rows, md):
    if md:
        out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
        out += ["| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |" for r in rows]
        return out
    w = [max(len(str(v)) for v in col) for col in zip(headers, *rows)]

    def fmt(r):
        return "  ".join(str(c).ljust(w[i]) for i, c in enumerate(r)).rstrip()

    return [fmt(headers), fmt(["-" * x for x in w])] + [fmt(r) for r in rows]


def status_label(st, la, lb):
    return {"same": "same", "changed": "CHANGED", "only_a": f"only in {la}", "only_b": f"only in {lb}"}[st]


def op_cell(v):
    if not v:
        return "-"
    if not v["version"]:
        return f"no CSV (sub {v['installed_csv'] or 'pending'})"
    s = v["version"] + (f" [{v['channel']}]" if v["channel"] else "")
    if v["phase"] and v["phase"] != "Succeeded":
        s += f" {v['phase']}"
    return s


def crd_cell(v):
    if not v:
        return "-"
    return f"storage {v['storage'] or '?'} / served {','.join(v['served']) or '-'}"


def counts(rows):
    c = Counter(r["status"] for r in rows)
    return c


def summary_text(rows, la, lb):
    c = counts(rows)
    parts = [f"{c[k]} {lbl}" for k, lbl in (("same", "same"), ("changed", "changed"),
                                            ("only_a", f"only in {la}"), ("only_b", f"only in {lb}")) if c[k]]
    return ", ".join(parts) or "none"


def render(report, fmt, show_all, max_fields):
    if fmt == "json":
        return json.dumps(report, indent=1)
    md = fmt == "markdown"
    la, lb = report["a"]["label"] or "A", report["b"]["label"] or "B"
    if la == lb:
        la, lb = f"A:{la}", f"B:{lb}"
    L = []

    def heading(text, level):
        if md:
            L.append("#" * level + " " + text)
        else:
            L.append(text)
            L.append(("=" if level <= 2 else "-") * len(text))
        L.append("")

    def bullet(text, indent=0):
        L.append(("  " * indent) + ("- " if md else "  - ") + text)

    heading(f"CRD / operator comparison: {la} vs {lb}", 1)
    meta_rows = [[k, str(report["a"].get(f) or "-"), str(report["b"].get(f) or "-")]
                 for k, f in (("API server", "server"), ("OCP version", "ocp_version"),
                              ("Snapshot taken", "taken_at"), ("Collected from", "source"))]
    L += table(["", la, lb], meta_rows, md)
    L.append("")
    if report["warnings"]:
        heading("Warnings", 2)
        for w in report["warnings"]:
            bullet(w)
        L.append("")

    for prod in report["products"].values():
        heading(prod["title"], 2)
        ops, crds, ins = prod["operators"], prod["crds"], prod["instances"]
        if not (ops or crds or ins):
            L.append("Not installed on either cluster.")
            L.append("")
            continue
        L.append(f"Operators: {summary_text(ops, la, lb)} | CRDs: {summary_text(crds, la, lb)} | "
                 f"Instances: {summary_text(ins, la, lb)}")
        L.append("")

        shown = [r for r in ops if show_all or r["status"] != "same"]
        if shown:
            heading("Operators", 3)
            L += table(["PACKAGE", la, lb, "STATUS"],
                       [[r["key"], op_cell(r["a"]), op_cell(r["b"]), status_label(r["status"], la, lb)]
                        for r in shown], md)
            L.append("")
            for r in shown:
                for ch in r["changes"]:
                    bullet(f"{r['key']}: {ch}")
            if any(r["changes"] for r in shown):
                L.append("")

        shown = [r for r in crds if show_all or r["status"] != "same"]
        if shown:
            heading("CRDs", 3)
            L += table(["CRD", la, lb, "STATUS"],
                       [[r["name"], crd_cell(r["a"]), crd_cell(r["b"]), status_label(r["status"], la, lb)]
                        for r in shown], md)
            L.append("")
            changed = [r for r in shown if r["changes"]]
            if changed:
                heading("CRD change details", 4)
                for r in changed:
                    bullet(f"**{r['name']}**" if md else r["name"])
                    for ch in r["changes"]:
                        bullet(ch, 1)
                    if max_fields:
                        for sd in r["schema"]:
                            for sign, items in (("+", sd["added"]), ("-", sd["removed"])):
                                for f in items[:max_fields]:
                                    bullet(f"{sign} {sd['version']} `{f}`" if md else f"{sign} {sd['version']} {f}", 2)
                                if len(items) > max_fields:
                                    bullet(f"... {len(items) - max_fields} more ({sign}); "
                                           "raise --fields or use --format json", 2)
                            for f, ta, tb in sd["retyped"][:max_fields]:
                                bullet(f"~ {sd['version']} {f}: {ta} -> {tb}", 2)
                L.append("")

        shown = [r for r in ins if show_all or r["status"] != "same"]
        if shown:
            heading("Instances", 3)

            def icell(f):
                return "-" if f is None else "; ".join(f"{k.split('.')[-1]}={'-' if v is None else v}" for k, v in f.items())

            L += table(["OBJECT", la, lb, "STATUS"],
                       [[r["key"], icell(r["a"]), icell(r["b"]), status_label(r["status"], la, lb)]
                        for r in shown], md)
            L.append("")

        if not show_all and all(r["status"] == "same" for r in ops + crds + ins):
            L.append("No differences.")
            L.append("")

    L.append("Result: " + ("DIFFERENCES FOUND" if report["has_differences"] else "identical"))
    return "\n".join(L) + "\n"


def render_show(snap, fmt):
    md = fmt == "markdown"
    m = snap_meta(snap)
    L = [f"Snapshot '{m['label']}' taken {m['taken_at']} from {m['server'] or 'redacted/unknown'} "
         f"(OCP {m['ocp_version'] or '?'}, source {m['source']})", ""]
    for n, d in snap.get("products", {}).items():
        ops = [o for o in snap["operators"] if o["product"] == n]
        crds = [c for c in snap["crds"] if c["product"] == n]
        L.append(f"{d.get('title', n)}: {len(crds)} CRDs")
        if ops:
            L += ["  " + l if not md else l for l in table(
                ["PACKAGE", "NAMESPACE", "VERSION"],
                [[o["package"], o["namespace"], op_cell(op_view(o))] for o in ops], md)]
        L.append("")
    for e in snap.get("errors", []):
        L.append(f"warning: could not read {e['resource']}: {e['error']}")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- print-commands

def print_commands(defs, cli):
    resources = [CRD_RES, CSV_RES, SUB_RES, CV_RES]
    resources += [p["resource"] for d in defs.values() for p in d.get("instances", [])]
    lines = [
        "#!/usr/bin/env bash",
        f"# Generated by crdcompare.py {TOOL_VERSION} print-commands.",
        f"# Run where '{cli}' is logged in to the cluster. Read-only (get / config view).",
        "# Usage: bash collect.sh <label>   -> <label>.tar.gz",
        "set -u",
        'OUT="${1:-crdcompare-$(date +%Y%m%d-%H%M%S)}"',
        'mkdir -p "$OUT"',
    ]
    for r in dict.fromkeys(resources):
        f = safe_name(r) + ".json"
        lines.append(f'{cli} get {r} -A -o json > "$OUT/{f}" 2>>"$OUT/errors.log" || rm -f "$OUT/{f}"')
    lines += [
        f"{cli} config view --minify -o jsonpath='{{.clusters[0].cluster.server}}' > \"$OUT/server.txt\" 2>/dev/null || true",
        'tar czf "$OUT.tar.gz" "$OUT" && echo "Wrote $OUT.tar.gz"',
    ]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- CLI

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version=TOOL_VERSION)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("snapshot", help="capture operator/CRD state of one cluster to a file")
    sp.add_argument("-o", "--output", help="output file (.json or .json.gz, '-' for stdout)")
    sp.add_argument("--label", help="name for this cluster in reports (default: from API URL)")
    sp.add_argument("--cli", default="oc", help="oc or kubectl (default: oc)")
    sp.add_argument("--kubeconfig")
    sp.add_argument("--context")
    sp.add_argument("--timeout", type=int, default=120, help="per-command timeout in seconds")
    sp.add_argument("--from-dir", metavar="PATH", help="build from JSON collected by print-commands (dir or .tar.gz)")
    sp.add_argument("--products-file", help="JSON file overriding/adding product definitions")
    sp.add_argument("--product", help="comma-separated subset, e.g. rhoai,service-mesh")
    sp.add_argument("--no-fields", action="store_true", help="store schema hashes only (smaller file, no field diff)")
    sp.add_argument("--redact", action="store_true", help="omit API server URL and cluster ID")

    cp = sub.add_parser("compare", help="compare two snapshot files (offline)")
    cp.add_argument("a")
    cp.add_argument("b")
    cp.add_argument("--format", choices=["text", "markdown", "json"], default="text")
    cp.add_argument("-o", "--output", help="write report to file instead of stdout")
    cp.add_argument("--all", action="store_true", help="also list identical operators/CRDs")
    cp.add_argument("--fields", type=int, default=15, help="max schema field changes listed per CRD version (0 hides)")
    cp.add_argument("--product", help="comma-separated subset of products to report")
    cp.add_argument("--exit-code", action="store_true", help="exit 1 when differences are found")

    shp = sub.add_parser("show", help="summarize a single snapshot")
    shp.add_argument("snapshot")
    shp.add_argument("--format", choices=["text", "markdown"], default="text")

    pc = sub.add_parser("print-commands", help="emit a bash collector for hosts without Python")
    pc.add_argument("--cli", default="oc")
    pc.add_argument("--products-file")
    pc.add_argument("--product")

    args = ap.parse_args(argv)

    if args.cmd == "snapshot":
        defs = load_products(args.products_file, args.product)
        src = open_dir_source(args.from_dir) if args.from_dir else \
            LiveSource(args.cli, args.kubeconfig, args.context, args.timeout)
        snap = take_snapshot(src, Products(defs), args.label, not args.no_fields, args.redact)
        out = args.output or f"crdcompare-{snap['label']}-{dt.datetime.now():%Y%m%d-%H%M%S}.json.gz"
        write_json(snap, out)
        if out != "-":
            eprint(f"wrote {out}: {len(snap['operators'])} operators, {len(snap['crds'])} CRDs, "
                   f"{len(snap['instances'])} instances, {len(snap['errors'])} read errors")
        return 0

    if args.cmd == "compare":
        filt = [p.strip() for p in args.product.split(",")] if args.product else None
        report = compare_snapshots(load_snapshot(args.a), load_snapshot(args.b), filt)
        text = render(report, args.format, args.all, args.fields)
        if args.output:
            with open(args.output, "w") as fh:
                fh.write(text)
            eprint(f"wrote {args.output}")
        else:
            sys.stdout.write(text)
        return 1 if (args.exit_code and report["has_differences"]) else 0

    if args.cmd == "show":
        sys.stdout.write(render_show(load_snapshot(args.snapshot), args.format))
        return 0

    if args.cmd == "print-commands":
        sys.stdout.write(print_commands(load_products(args.products_file, args.product), args.cli))
        return 0
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
