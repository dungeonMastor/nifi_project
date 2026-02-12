"""NiFi REST API tools: list processor types and controller-service instances."""

import json
import logging
import os
import re
import ssl
import uuid
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NiFi config (from env — same vars as planValidator / build_nifi_pipeline)
# ---------------------------------------------------------------------------
NIFI_BASE_URL = (os.environ.get("NIFI_BASE_URL")).rstrip("/")
NIFI_AUTH = os.environ.get("NIFI_AUTH", "")
NIFI_VERIFY_SSL = os.environ.get("NIFI_VERIFY_SSL", "true").strip().lower() not in (
    "0", "false", "no", "off",
)


def _nifi_api_request(method: str, path: str, body: dict | None = None, auth: str = "") -> dict:
    """Send a request to the NiFi REST API.  Returns parsed JSON dict."""
    url = f"{NIFI_BASE_URL}{path}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if auth:
        headers["Authorization"] = f"Bearer {auth}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = Request(url, data=data, headers=headers, method=method)

    ctx = None
    if not NIFI_VERIFY_SSL:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    try:
        with urlopen(req, timeout=60, context=ctx) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        raise RuntimeError(f"NiFi API error {e.code} {e.reason}: {url}\n{err_body}") from e
    except URLError as e:
        raise RuntimeError(f"NiFi request failed: {url} -> {e.reason}") from e


# ---------------------------------------------------------------------------
# Public helpers (also exposed as MCP tools via main.py)
# ---------------------------------------------------------------------------

def list_nifi_processor_types(auth: str = "") -> list[dict]:
    """Return available processor types from the NiFi instance.

    Each entry: ``{"type": "<FQCN>", "bundle": {"group": ..., "artifact": ..., "version": ...}}``.
    """
    auth = auth or NIFI_AUTH
    if not auth:
        return []
    out = _nifi_api_request("GET", "/nifi-api/flow/processor-types", auth=auth)
    items = out.get("processorTypes") if isinstance(out, dict) else []
    result: list[dict] = []
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        fqcn = item.get("type")
        if not fqcn:
            continue
        entry: dict = {"type": fqcn}
        bundle = item.get("bundle")
        if isinstance(bundle, dict):
            entry["bundle"] = bundle
        result.append(entry)
    return result


def list_nifi_cs_types(auth: str = "") -> set[str]:
    """Return the set of installed controller-service type FQCNs from the NiFi instance.

    Uses ``/nifi-api/flow/controller-service-types``.  This tells us which
    FQCNs are *valid* CS types so we can distinguish a property value like
    ``org.apache.nifi.dbcp.DBCPConnectionPool`` (CS reference) from ``Text``
    (regular config value).
    """
    auth = auth or NIFI_AUTH
    if not auth:
        return set()
    out = _nifi_api_request("GET", "/nifi-api/flow/controller-service-types", auth=auth)
    items = out.get("controllerServiceTypes") if isinstance(out, dict) else []
    result: set[str] = set()
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        fqcn = item.get("type")
        if fqcn:
            result.add(fqcn)
    return result


def list_nifi_controller_services(auth: str = "") -> list[dict]:
    """Return existing controller-service **instances** from the NiFi root process group.

    Each entry: ``{"id": "<uuid>", "type": "<FQCN>", "name": "<display name>", "state": "ENABLED|DISABLED|..."}``.
    """
    auth = auth or NIFI_AUTH
    if not auth:
        return []

    # Resolve root PG id
    root_entity = _nifi_api_request("GET", "/nifi-api/flow/process-groups/root", auth=auth)
    if "processGroupFlow" in root_entity:
        root_id = root_entity["processGroupFlow"]["id"]
    elif "component" in root_entity and "id" in root_entity["component"]:
        root_id = root_entity["component"]["id"]
    else:
        root_id = root_entity.get("id") or "root"

    out = _nifi_api_request(
        "GET",
        f"/nifi-api/flow/process-groups/{root_id}/controller-services",
        auth=auth,
    )
    items = out.get("controllerServices") if isinstance(out, dict) else []
    result: list[dict] = []
    for ent in (items or []):
        if not isinstance(ent, dict):
            continue
        comp = ent.get("component") if isinstance(ent.get("component"), dict) else {}
        cs_id = comp.get("id")
        cs_type = comp.get("type")
        cs_name = comp.get("name")
        cs_state = comp.get("state", "UNKNOWN")
        if cs_id and cs_type:
            result.append({
                "id": str(cs_id),
                "type": str(cs_type),
                "name": str(cs_name or ""),
                "state": str(cs_state),
            })
    return result


def get_nifi_version(auth: str = "") -> Optional[str]:
    """Return the NiFi instance version string (e.g. '2.7.2') from GET /nifi-api/flow/about, or None on failure."""
    auth = auth or NIFI_AUTH
    if not auth:
        return None
    try:
        out = _nifi_api_request("GET", "/nifi-api/flow/about", auth=auth)
    except Exception as e:
        logger.debug("Could not fetch NiFi version: %s", e)
        return None
    if not isinstance(out, dict):
        return None
    version = (out.get("about") or {}).get("version") if isinstance(out.get("about"), dict) else None
    if version is None:
        version = out.get("version")
    return str(version).strip() if version else None


def build_nifi_types_context(auth: str = "") -> str:
    """Build a compact text block of existing controller-service instances for LLM prompt injection.

    Replaces ``planValidator.fetch_nifi_types_context``.
    NiFi version is not included; do not add version context to prompts.
    """
    cs_instances = list_nifi_controller_services(auth)

    lines: list[str] = []

    if cs_instances:
        lines.append(f"Existing controller-service instances ({len(cs_instances)} total):")
        for cs in cs_instances:
            lines.append(f"  - {cs['name']} (type: {cs['type']}, state: {cs['state']}, id: {cs['id']})")

    return "\n".join(lines) if lines else "(Could not fetch NiFi types.)"


# ---------------------------------------------------------------------------
# Property descriptor discovery (for property-name validation)
# ---------------------------------------------------------------------------

def _normalize_prop_name(name: str) -> str:
    """Strip non-alphanumeric chars and lowercase for fuzzy property-name matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _client_id() -> str:
    return str(uuid.uuid4())


def _get_root_pg_id(auth: str) -> str:
    """Return the root process group id (reuses cached list_nifi_controller_services logic)."""
    root_entity = _nifi_api_request("GET", "/nifi-api/flow/process-groups/root", auth=auth)
    if "processGroupFlow" in root_entity:
        return root_entity["processGroupFlow"]["id"]
    if "component" in root_entity and "id" in root_entity["component"]:
        return root_entity["component"]["id"]
    return root_entity.get("id") or "root"


def fetch_processor_property_descriptors(
    proc_types: list[dict],
    auth: str = "",
) -> dict[str, dict[str, dict]]:
    """Fetch property descriptors for each processor type by creating temporary processors.

    Creates a temp process group, creates one processor per unique type+bundle,
    extracts ``component.config.descriptors`` from each creation response,
    then deletes the temp PG (cascading delete removes all temp processors).

    Args:
        proc_types: list of ``{"type": "<FQCN>", "bundle": {...}}`` dicts.
        auth: NiFi Bearer token.

    Returns:
        ``{fqcn: {prop_name: {"name": ..., "identifiesControllerService": bool, ...}}}``
    """
    auth = auth or NIFI_AUTH
    if not auth:
        return {}

    # De-duplicate by FQCN (keep first bundle seen)
    seen: dict[str, dict] = {}
    for pt in proc_types:
        fqcn = pt.get("type", "")
        if fqcn and fqcn not in seen:
            seen[fqcn] = pt.get("bundle") or {
                "group": "org.apache.nifi",
                "artifact": "nifi-standard-nar",
                "version": "1.21.0",
            }

    if not seen:
        return {}

    root_id = _get_root_pg_id(auth)

    # 1. Create temp PG
    temp_pg_name = f"__planvalidator_temp_{uuid.uuid4().hex[:8]}"
    pg_payload = {
        "revision": {"clientId": _client_id(), "version": 0},
        "component": {
            "name": temp_pg_name,
            "position": {"x": 0, "y": 0},
        },
    }
    pg_entity = _nifi_api_request(
        "POST",
        f"/nifi-api/process-groups/{root_id}/process-groups",
        body=pg_payload,
        auth=auth,
    )
    temp_pg_id = (pg_entity.get("component") or {}).get("id") or pg_entity.get("id")
    if not temp_pg_id:
        raise RuntimeError("Failed to create temp process group for descriptor discovery")

    result: dict[str, dict[str, dict]] = {}

    try:
        # 2. For each unique processor type, create a temp processor and extract descriptors
        for fqcn, bundle in seen.items():
            proc_payload = {
                "revision": {"clientId": _client_id(), "version": 0},
                "component": {
                    "parentGroupId": temp_pg_id,
                    "name": f"__temp_{fqcn.rsplit('.', 1)[-1]}",
                    "type": fqcn,
                    "bundle": bundle,
                    "position": {"x": 0, "y": 0},
                    "config": {"properties": {}},
                },
            }
            try:
                proc_entity = _nifi_api_request(
                    "POST",
                    f"/nifi-api/process-groups/{temp_pg_id}/processors",
                    body=proc_payload,
                    auth=auth,
                )
            except RuntimeError as e:
                logger.warning("Could not create temp processor for %s: %s", fqcn, e)
                continue

            # Extract descriptors from the response
            comp = proc_entity.get("component") or {}
            config = comp.get("config") or {}
            descriptors = config.get("descriptors") or {}

            # Each descriptor: { "name": "...", "displayName": "...",
            #                    "identifiesControllerService": bool, ... }
            type_descriptors: dict[str, dict] = {}
            for prop_name, desc in descriptors.items():
                if isinstance(desc, dict):
                    type_descriptors[prop_name] = desc

            result[fqcn] = type_descriptors

    finally:
        # 3. Delete temp PG (cascading — removes all temp processors)
        try:
            pg_info = _nifi_api_request("GET", f"/nifi-api/process-groups/{temp_pg_id}", auth=auth)
            version = (pg_info.get("revision") or {}).get("version", 0)
            cid = _client_id()
            _nifi_api_request(
                "DELETE",
                f"/nifi-api/process-groups/{temp_pg_id}?version={version}&clientId={cid}",
                auth=auth,
            )
        except Exception as e:
            logger.warning("Failed to delete temp PG %s: %s", temp_pg_id, e)

    return result
