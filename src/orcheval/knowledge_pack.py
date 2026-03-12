from __future__ import annotations

import copy
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from importlib import metadata as importlib_metadata
except Exception:  # pragma: no cover
    importlib_metadata = None  # type: ignore


PACK_SCHEMA_VERSION = "1.0.0"
DEFAULT_PACK_FILENAME = "default_pack_v1.json"
VALID_MODES = {"legacy", "pack", "auto"}
ORCHESTRATOR_PACKAGES = {
    "airflow": "apache-airflow",
    "prefect": "prefect",
    "dagster": "dagster",
}


@dataclass
class KnowledgePackResolution:
    mode_selected: str
    mode_effective: str
    status: str
    applied: bool
    orchestrator: str
    orchestrator_version: Optional[str]
    resolved_version: Optional[str] = None
    pack_id: Optional[str] = None
    pack_version: Optional[str] = None
    schema_version: Optional[str] = None
    pack_path: Optional[str] = None
    source: str = "none"
    capabilities: Dict[str, Any] = field(default_factory=dict)
    rule_params: Dict[str, Any] = field(default_factory=dict)
    uncertainty: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode_selected": self.mode_selected,
            "mode_effective": self.mode_effective,
            "status": self.status,
            "applied": bool(self.applied),
            "orchestrator": self.orchestrator,
            "orchestrator_version": self.orchestrator_version,
            "resolved_version": self.resolved_version,
            "pack_id": self.pack_id,
            "pack_version": self.pack_version,
            "schema_version": self.schema_version,
            "pack_path": self.pack_path,
            "source": self.source,
            "capabilities": self.capabilities,
            "rule_params": self.rule_params,
            "uncertainty": self.uncertainty,
            "errors": self.errors,
        }


class KnowledgePackError(Exception):
    pass


def bundled_pack_path() -> Path:
    return Path(__file__).resolve().parent / "knowledge_packs" / DEFAULT_PACK_FILENAME


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_mode(mode: Optional[str]) -> str:
    raw = (mode or "legacy").strip().lower()
    return raw if raw in VALID_MODES else "legacy"


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(_as_dict(base))
    for key, value in _as_dict(override).items():
        if isinstance(out.get(key), dict) and isinstance(value, dict):
            out[key] = _deep_merge(_as_dict(out.get(key)), value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _validate_pack(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise KnowledgePackError("knowledge_pack_not_object")

    required = ["pack_id", "pack_version", "schema_version", "orchestrators"]
    missing = [k for k in required if k not in payload]
    if missing:
        raise KnowledgePackError(f"knowledge_pack_missing_keys:{','.join(missing)}")

    if str(payload.get("schema_version")) != PACK_SCHEMA_VERSION:
        raise KnowledgePackError(
            f"knowledge_pack_schema_mismatch:expected={PACK_SCHEMA_VERSION},got={payload.get('schema_version')}"
        )

    orchestrators = payload.get("orchestrators")
    if not isinstance(orchestrators, dict):
        raise KnowledgePackError("knowledge_pack_orchestrators_not_object")

    for orch, section in orchestrators.items():
        if not isinstance(section, dict):
            raise KnowledgePackError(f"knowledge_pack_orchestrator_section_invalid:{orch}")
        generic = section.get("generic")
        versions = section.get("versions")
        if generic is None or not isinstance(generic, dict):
            raise KnowledgePackError(f"knowledge_pack_generic_missing:{orch}")
        if versions is None or not isinstance(versions, dict):
            raise KnowledgePackError(f"knowledge_pack_versions_missing:{orch}")


def load_knowledge_pack(path: Optional[Path], *, required_pack_version: Optional[str] = None) -> Tuple[Dict[str, Any], Path, str]:
    selected = Path(path) if path else bundled_pack_path()
    if not selected.exists():
        raise KnowledgePackError(f"knowledge_pack_not_found:{selected}")

    try:
        payload = json.loads(selected.read_text(encoding="utf-8"))
    except Exception as e:
        raise KnowledgePackError(f"knowledge_pack_parse_error:{type(e).__name__}:{e}")

    _validate_pack(payload)

    if required_pack_version and str(payload.get("pack_version")) != str(required_pack_version):
        raise KnowledgePackError(
            f"knowledge_pack_version_mismatch:expected={required_pack_version},got={payload.get('pack_version')}"
        )

    source = "user_path" if path else "bundled"
    return payload, selected, source


def validate_knowledge_pack(payload: Dict[str, Any]) -> None:
    _validate_pack(payload)


def _parse_version_tuple(text: Optional[str]) -> Optional[Tuple[int, ...]]:
    if not text:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    parts: List[int] = []
    for tok in raw.split("."):
        digits = ""
        for ch in tok:
            if ch.isdigit():
                digits += ch
            else:
                break
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts) if parts else None


def _match_version_key(version_map: Dict[str, Any], detected_version: Optional[str]) -> Tuple[Optional[str], str]:
    if not isinstance(version_map, dict):
        return None, "none"
    if not detected_version:
        return None, "none"

    v = str(detected_version).strip()
    if not v:
        return None, "none"

    if v in version_map:
        return v, "exact"

    parts = v.split(".")
    candidates: List[str] = []
    if len(parts) >= 2:
        candidates.append(".".join(parts[:2]))
    if len(parts) >= 1:
        candidates.append(parts[0])

    for c in candidates:
        if c in version_map:
            return c, "exact"

    # Approximate policy: same major, nearest known minor.
    detected_tuple = _parse_version_tuple(v)
    if detected_tuple and len(detected_tuple) >= 1:
        detected_major = detected_tuple[0]
        detected_minor = detected_tuple[1] if len(detected_tuple) >= 2 else 0
        nearest_key: Optional[str] = None
        nearest_distance: Optional[int] = None

        for key in sorted(version_map.keys()):
            parsed = _parse_version_tuple(key)
            if not parsed:
                continue
            if parsed[0] != detected_major:
                continue
            key_minor = parsed[1] if len(parsed) >= 2 else 0
            dist = abs(key_minor - detected_minor)
            if nearest_distance is None or dist < nearest_distance:
                nearest_key = key
                nearest_distance = dist

        if nearest_key is not None:
            return nearest_key, "approximate"

    return None, "none"


def resolve_knowledge_pack(
    *,
    orchestrator: str,
    orchestrator_version: Optional[str],
    mode: Optional[str],
    pack_path: Optional[Path],
    pack_version: Optional[str],
) -> KnowledgePackResolution:
    mode_effective = _normalize_mode(mode)
    orch = (orchestrator or "unknown").strip().lower()

    if mode_effective == "legacy":
        return KnowledgePackResolution(
            mode_selected=str(mode or "legacy"),
            mode_effective=mode_effective,
            status="legacy_bypass",
            applied=False,
            orchestrator=orch,
            orchestrator_version=orchestrator_version,
        )

    try:
        payload, used_path, source = load_knowledge_pack(pack_path, required_pack_version=pack_version)
    except Exception as e:
        status = "pack_unavailable_fallback" if mode_effective == "auto" else "pack_unavailable"
        return KnowledgePackResolution(
            mode_selected=str(mode or "legacy"),
            mode_effective=mode_effective,
            status=status,
            applied=False,
            orchestrator=orch,
            orchestrator_version=orchestrator_version,
            pack_path=str(pack_path) if pack_path else str(bundled_pack_path()),
            errors=[{"error_type": type(e).__name__, "message": str(e)}],
            uncertainty=[
                {
                    "code": "knowledge_pack_unavailable",
                    "severity": "major",
                    "message": "Pack could not be loaded; evaluator fell back to conservative core behavior.",
                }
            ],
        )

    orch_section = _as_dict(_as_dict(payload.get("orchestrators")).get(orch))
    if not orch_section:
        return KnowledgePackResolution(
            mode_selected=str(mode or "legacy"),
            mode_effective=mode_effective,
            status="unsupported_orchestrator",
            applied=False,
            orchestrator=orch,
            orchestrator_version=orchestrator_version,
            pack_id=str(payload.get("pack_id")),
            pack_version=str(payload.get("pack_version")),
            schema_version=str(payload.get("schema_version")),
            pack_path=str(used_path),
            source=source,
            uncertainty=[
                {
                    "code": "unsupported_orchestrator",
                    "severity": "major",
                    "message": f"No knowledge-pack section exists for orchestrator={orch}.",
                }
            ],
        )

    generic = _as_dict(orch_section.get("generic"))
    versions = _as_dict(orch_section.get("versions"))
    matched_key, match_kind = _match_version_key(versions, orchestrator_version)

    if matched_key is None:
        return KnowledgePackResolution(
            mode_selected=str(mode or "legacy"),
            mode_effective=mode_effective,
            status="unknown_version_conservative",
            applied=False,
            orchestrator=orch,
            orchestrator_version=orchestrator_version,
            pack_id=str(payload.get("pack_id")),
            pack_version=str(payload.get("pack_version")),
            schema_version=str(payload.get("schema_version")),
            pack_path=str(used_path),
            source=source,
            capabilities=_as_dict(generic.get("capabilities")),
            rule_params={},
            uncertainty=[
                {
                    "code": "unknown_orchestrator_version",
                    "severity": "major",
                    "message": "Orchestrator version is unknown to the pack; conservative core checks only.",
                    "detected_version": orchestrator_version,
                }
            ],
        )

    merged = _deep_merge(generic, _as_dict(versions.get(matched_key)))
    uncertainty: List[Dict[str, Any]] = []
    status = "resolved"
    if match_kind == "approximate":
        status = "resolved_approximate"
        uncertainty.append(
            {
                "code": "approximate_version_match",
                "severity": "minor",
                "message": "Closest supported version was used for this orchestrator; results may be less accurate.",
                "detected_version": orchestrator_version,
                "resolved_version": matched_key,
            }
        )
    return KnowledgePackResolution(
        mode_selected=str(mode or "legacy"),
        mode_effective=mode_effective,
        status=status,
        applied=True,
        orchestrator=orch,
        orchestrator_version=orchestrator_version,
        resolved_version=matched_key,
        pack_id=str(payload.get("pack_id")),
        pack_version=str(payload.get("pack_version")),
        schema_version=str(payload.get("schema_version")),
        pack_path=str(used_path),
        source=source,
        capabilities=_as_dict(merged.get("capabilities")),
        rule_params=_as_dict(merged.get("rule_params")),
        uncertainty=uncertainty,
    )


def resolve_rule_value(
    resolution: Optional[KnowledgePackResolution],
    *,
    check_id: str,
    default: Any,
    capability_refs: Optional[List[str]] = None,
) -> Tuple[Any, Dict[str, Any]]:
    refs = list(capability_refs or [])
    if resolution is None or not isinstance(resolution, KnowledgePackResolution):
        return default, {
            "check_id": check_id,
            "rule_source": "core",
            "capability_refs": refs,
            "knowledge_pack_applied": False,
            "resolution_status": "no_resolution",
        }

    use_pack = bool(resolution.applied)
    found = use_pack and check_id in resolution.rule_params
    value = resolution.rule_params.get(check_id, default) if found else default

    provenance = {
        "check_id": check_id,
        "rule_source": "pack" if found else "core",
        "capability_refs": refs,
        "knowledge_pack_applied": use_pack,
        "resolution_status": resolution.status,
        "pack_id": resolution.pack_id,
        "pack_version": resolution.pack_version,
    }
    return value, provenance


def detect_orchestrator_version(
    orchestrator: str,
    *,
    python_exe: Optional[Path] = None,
    timeout_s: int = 8,
) -> Optional[str]:
    orch = (orchestrator or "").strip().lower()
    package = ORCHESTRATOR_PACKAGES.get(orch)
    if not package:
        return None

    if python_exe is None or Path(python_exe).resolve() == Path(sys.executable).resolve():
        if importlib_metadata is None:
            return None
        try:
            return importlib_metadata.version(package)
        except Exception:
            return None

    code = (
        "import importlib.metadata as m\n"
        f"name={json.dumps(package)}\n"
        "try:\n"
        "  print(m.version(name))\n"
        "except Exception:\n"
        "  pass\n"
    )
    try:
        proc = subprocess.run(
            [str(python_exe), "-c", code],
            capture_output=True,
            text=True,
            check=False,
            timeout=int(timeout_s),
        )
        out = (proc.stdout or "").strip()
        return out or None
    except Exception:
        return None


def resolution_warnings(resolution: Optional[KnowledgePackResolution]) -> List[Dict[str, Any]]:
    if resolution is None:
        return []
    warnings: List[Dict[str, Any]] = []
    for item in list(resolution.uncertainty or []):
        if not isinstance(item, dict):
            continue
        warnings.append(
            {
                "source": "knowledge_pack",
                "code": str(item.get("code") or "knowledge_pack_warning"),
                "severity": str(item.get("severity") or "info"),
                "message": str(item.get("message") or ""),
                "details": item,
            }
        )
    return warnings
