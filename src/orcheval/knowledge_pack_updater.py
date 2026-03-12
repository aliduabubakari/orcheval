#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .knowledge_pack import bundled_pack_path, load_knowledge_pack, validate_knowledge_pack


def _as_dict(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = json.loads(json.dumps(_as_dict(base)))
    for key, value in _as_dict(overlay).items():
        if isinstance(out.get(key), dict) and isinstance(value, dict):
            out[key] = _deep_merge(_as_dict(out.get(key)), value)
        else:
            out[key] = value
    return out


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _collect_diff_paths(left: Any, right: Any, prefix: str = "") -> List[str]:
    paths: List[str] = []
    if type(left) != type(right):
        paths.append(prefix or "$")
        return paths

    if isinstance(left, dict):
        keys = sorted(set(left.keys()) | set(right.keys()))
        for k in keys:
            p = f"{prefix}.{k}" if prefix else str(k)
            if k not in left or k not in right:
                paths.append(p)
            else:
                paths.extend(_collect_diff_paths(left[k], right[k], p))
        return paths

    if isinstance(left, list):
        if len(left) != len(right):
            paths.append(prefix or "$")
            return paths
        for idx, (lv, rv) in enumerate(zip(left, right)):
            p = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
            paths.extend(_collect_diff_paths(lv, rv, p))
        return paths

    if left != right:
        paths.append(prefix or "$")
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Build candidate knowledge-pack update artifact")
    parser.add_argument("--base-pack", default=None, help="Base pack path (default bundled pack)")
    parser.add_argument("--snapshot", default=None, help="Curated snapshot JSON to merge into base pack")
    parser.add_argument("--out-dir", default="orcheval_reports/knowledge_packs", help="Output folder for candidate artifacts")
    parser.add_argument("--candidate-name", default="candidate_pack.json")
    parser.add_argument("--report-name", default="candidate_report.json")
    args = parser.parse_args()

    base_path = Path(args.base_pack) if args.base_pack else bundled_pack_path()
    base_payload, used_path, _src = load_knowledge_pack(base_path)

    overlay_payload: Dict[str, Any] = {}
    if args.snapshot:
        snap_path = Path(args.snapshot)
        overlay_payload = json.loads(snap_path.read_text(encoding="utf-8"))
        if not isinstance(overlay_payload, dict):
            raise SystemExit("snapshot must be a JSON object")

    candidate_payload = _deep_merge(base_payload, overlay_payload)
    candidate_payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    candidate_payload.setdefault("sources_digest", {})
    candidate_payload["sources_digest"] = {
        **_as_dict(candidate_payload.get("sources_digest")),
        "update_source": "offline_curated",
        "base_pack": str(used_path),
        "snapshot": str(args.snapshot) if args.snapshot else None,
    }

    validate_knowledge_pack(candidate_payload)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = out_dir / args.candidate_name
    report_path = out_dir / args.report_name

    base_txt = json.dumps(base_payload, sort_keys=True, indent=2)
    candidate_txt = json.dumps(candidate_payload, sort_keys=True, indent=2)

    candidate_path.write_text(candidate_txt + "\n", encoding="utf-8")

    diff_paths = _collect_diff_paths(base_payload, candidate_payload)
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "base_pack": str(used_path),
        "candidate_pack": str(candidate_path),
        "base_sha256": _sha256_text(base_txt),
        "candidate_sha256": _sha256_text(candidate_txt),
        "change_count": len(diff_paths),
        "changed_paths": diff_paths,
        "validation": {"schema_valid": True},
    }
    report_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote candidate pack: {candidate_path}")
    print(f"Wrote review report: {report_path}")


if __name__ == "__main__":
    main()
