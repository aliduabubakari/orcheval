from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from ..base_evaluator import Orchestrator
from ..knowledge_pack import detect_orchestrator_version, resolution_warnings, resolve_knowledge_pack
from ..subprocess_json_runner import run_cli_json
from .design_rules import EcoDesignRuleEngine
from .structural_energy_analyzer import StructuralEnergyAnalyzer


SCHEMA_VERSION = "1.0.0"
RECIPE_SCHEMA_VERSION = "1"
DEFAULT_SEED = 1729
RUNTIME_SUPPORTED = {"airflow", "prefect", "dagster"}


class LLMProviderError(Exception):
    def __init__(self, code: str, message: str, *, provider: Optional[str] = None, retryable: bool = False, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.provider = provider
        self.retryable = bool(retryable)
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "provider": self.provider,
            "retryable": self.retryable,
            "details": self.details,
        }


class LLMProvider(Protocol):
    provider_name: str

    def health_check(self) -> Tuple[bool, Optional[str]]:
        ...

    def generate_synthetic_spec(self, minimized_spec: Dict[str, Any], *, seed: int, max_rows: int) -> Dict[str, Any]:
        ...


@dataclass
class EnergyEvaluationRequest:
    file_path: Path
    code_text: str
    orchestrator: str
    repo_root: Path
    runtime_python: Optional[Path] = None
    mode_selected: str = "auto"
    sample_paths: List[Path] = field(default_factory=list)
    max_rows: int = 500
    max_tasks: int = 25
    timeout_s: int = 120
    seed: int = DEFAULT_SEED
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key_env: Optional[str] = None
    llm_base_url: Optional[str] = None
    llm_timeout_s: int = 30
    execution_adapter: str = "representative"
    orchestrator_version: Optional[str] = None
    knowledge_pack_mode: str = "legacy"
    knowledge_pack: Optional[Path] = None
    knowledge_pack_version: Optional[str] = None
    artifacts_dir: Optional[Path] = None


@dataclass
class EnergyEvaluationResult:
    mode_selected: str
    mode_used: str
    data_source: str
    attempted_modes: List[Dict[str, Any]]
    runtime_measurement: Optional[Dict[str, Any]]
    synthetic_info: Optional[Dict[str, Any]]
    privacy: Dict[str, Any]
    heuristic: Dict[str, Any]
    errors: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "mode_selected": self.mode_selected,
            "mode_used": self.mode_used,
            "data_source": self.data_source,
            "attempted_modes": self.attempted_modes,
            "runtime_measurement": self.runtime_measurement,
            "synthetic_info": self.synthetic_info,
            "privacy": self.privacy,
            "heuristic": self.heuristic,
            "errors": self.errors,
            "metadata": self.metadata,
        }


class OpenAICompatibleProvider:
    DEFAULT_URLS = {
        "openai": "https://api.openai.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
        "deepinfra": "https://api.deepinfra.com/v1/openai",
        "deepseek": "https://api.deepseek.com/v1",
    }
    KEY_ENVS = {
        "openai": "OPENAI_API_KEY",
        "openrouter": "OPENROUTER_API_KEY",
        "deepinfra": "DEEPINFRA_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
    }

    def __init__(self, provider_name: str, *, model: str, api_key_env: Optional[str], base_url: Optional[str], timeout_s: int):
        self.provider_name = provider_name
        self.model = model
        self.api_key_env = api_key_env or self.KEY_ENVS.get(provider_name, "OPENAI_API_KEY")
        self.base_url = (base_url or self.DEFAULT_URLS.get(provider_name) or "").rstrip("/")
        self.timeout_s = int(timeout_s)
        self.api_key = os.environ.get(self.api_key_env)

    def health_check(self) -> Tuple[bool, Optional[str]]:
        if not self.base_url:
            return False, "missing_base_url"
        if not self.api_key:
            return False, f"missing_api_key_env:{self.api_key_env}"
        if not self.model:
            return False, "missing_model"
        return True, None

    def _http_post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode("utf-8", errors="ignore")
            raise LLMProviderError(
                "http_error",
                f"HTTP {e.code}",
                provider=self.provider_name,
                retryable=(e.code >= 500),
                details={"status_code": e.code, "body": body_txt[-1000:]},
            )
        except urllib.error.URLError as e:
            raise LLMProviderError("network_error", str(e), provider=self.provider_name, retryable=True)
        except json.JSONDecodeError as e:
            raise LLMProviderError("invalid_json", str(e), provider=self.provider_name)

    @staticmethod
    def _extract_json_block(text: str) -> Dict[str, Any]:
        txt = (text or "").strip()
        if not txt:
            raise LLMProviderError("empty_response", "empty model response")
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        l = txt.find("{")
        r = txt.rfind("}")
        if l >= 0 and r > l:
            block = txt[l : r + 1]
            try:
                obj = json.loads(block)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass
        raise LLMProviderError("invalid_response", "model response is not valid JSON object")

    def generate_synthetic_spec(self, minimized_spec: Dict[str, Any], *, seed: int, max_rows: int) -> Dict[str, Any]:
        ok, err = self.health_check()
        if not ok:
            raise LLMProviderError("health_check_failed", err or "provider not ready", provider=self.provider_name)

        prompt = {
            "task": "Create deterministic synthetic tabular data recipe for workflow energy benchmarking.",
            "constraints": {
                "no_sensitive_data": True,
                "deterministic_seed": int(seed),
                "max_rows": int(max_rows),
                "output_schema": {
                    "row_count": "int",
                    "columns": [{"name": "string", "type": "int|float|str|bool|date", "example": "any"}],
                },
            },
            "workflow_spec": minimized_spec,
        }

        user_msg = (
            "Return only JSON object with fields row_count and columns. "
            "No markdown, no prose. Keep columns 3-10 and row_count <= max_rows. "
            f"Seed: {seed}."
        )

        body = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": json.dumps(prompt, sort_keys=True)},
                {"role": "user", "content": user_msg},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = self._http_post_json(f"{self.base_url}/chat/completions", body, headers)
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception as e:
            raise LLMProviderError("invalid_response", f"missing choices/message/content: {e}", provider=self.provider_name)

        out = self._extract_json_block(content)
        return out


class AnthropicProvider:
    DEFAULT_URL = "https://api.anthropic.com/v1"

    def __init__(self, *, model: str, api_key_env: Optional[str], base_url: Optional[str], timeout_s: int):
        self.provider_name = "anthropic"
        self.model = model
        self.api_key_env = api_key_env or "ANTHROPIC_API_KEY"
        self.base_url = (base_url or self.DEFAULT_URL).rstrip("/")
        self.timeout_s = int(timeout_s)
        self.api_key = os.environ.get(self.api_key_env)

    def health_check(self) -> Tuple[bool, Optional[str]]:
        if not self.base_url:
            return False, "missing_base_url"
        if not self.api_key:
            return False, f"missing_api_key_env:{self.api_key_env}"
        if not self.model:
            return False, "missing_model"
        return True, None

    def _http_post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode("utf-8", errors="ignore")
            raise LLMProviderError(
                "http_error",
                f"HTTP {e.code}",
                provider=self.provider_name,
                retryable=(e.code >= 500),
                details={"status_code": e.code, "body": body_txt[-1000:]},
            )
        except urllib.error.URLError as e:
            raise LLMProviderError("network_error", str(e), provider=self.provider_name, retryable=True)
        except json.JSONDecodeError as e:
            raise LLMProviderError("invalid_json", str(e), provider=self.provider_name)

    def generate_synthetic_spec(self, minimized_spec: Dict[str, Any], *, seed: int, max_rows: int) -> Dict[str, Any]:
        ok, err = self.health_check()
        if not ok:
            raise LLMProviderError("health_check_failed", err or "provider not ready", provider=self.provider_name)

        prompt = {
            "task": "Create deterministic synthetic tabular data recipe for workflow energy benchmarking.",
            "constraints": {
                "no_sensitive_data": True,
                "deterministic_seed": int(seed),
                "max_rows": int(max_rows),
                "output_schema": {
                    "row_count": "int",
                    "columns": [{"name": "string", "type": "int|float|str|bool|date", "example": "any"}],
                },
            },
            "workflow_spec": minimized_spec,
        }

        body = {
            "model": self.model,
            "max_tokens": 1200,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Return only JSON object with fields row_count and columns. "
                        "No markdown, no prose. Keep columns 3-10 and row_count <= max_rows. "
                        f"Seed: {seed}. Context: {json.dumps(prompt, sort_keys=True)}"
                    ),
                }
            ],
        }
        headers = {
            "x-api-key": self.api_key or "",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        resp = self._http_post_json(f"{self.base_url}/messages", body, headers)
        try:
            content = resp["content"][0]["text"]
        except Exception as e:
            raise LLMProviderError("invalid_response", f"missing content text: {e}", provider=self.provider_name)

        return OpenAICompatibleProvider._extract_json_block(content)


def build_provider(name: str, *, model: str, api_key_env: Optional[str], base_url: Optional[str], timeout_s: int) -> LLMProvider:
    key = (name or "").strip().lower()
    if key in {"openai", "openrouter", "deepinfra", "deepseek"}:
        return OpenAICompatibleProvider(key, model=model, api_key_env=api_key_env, base_url=base_url, timeout_s=timeout_s)
    if key in {"anthropic", "claude"}:
        return AnthropicProvider(model=model, api_key_env=api_key_env, base_url=base_url, timeout_s=timeout_s)
    raise LLMProviderError("unsupported_provider", f"Unsupported provider: {name}", provider=name)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _normalize_rows(rows: List[Dict[str, Any]], max_rows: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows[: max(0, int(max_rows))]:
        if not isinstance(r, dict):
            continue
        out.append({str(k): v for k, v in r.items()})
    return out


def _load_json_rows(path: Path) -> List[Dict[str, Any]]:
    obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


def _load_jsonl_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def _load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def _discover_sample_files(paths: List[Path]) -> List[Path]:
    files: List[Path] = []
    for p in paths:
        if p.is_file():
            files.append(p)
            continue
        if p.is_dir():
            for c in sorted(p.rglob("*")):
                if c.is_file() and c.suffix.lower() in {".csv", ".json", ".jsonl"}:
                    files.append(c)
    dedup: List[Path] = []
    seen = set()
    for f in files:
        k = str(f.resolve())
        if k not in seen:
            dedup.append(f)
            seen.add(k)
    return dedup


def load_sample_dataset(paths: List[Path], *, max_rows: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    provenance: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    files = _discover_sample_files(paths)
    for path in files:
        fmt = path.suffix.lower().lstrip(".")
        try:
            if path.suffix.lower() == ".csv":
                loaded = _load_csv_rows(path)
            elif path.suffix.lower() == ".json":
                loaded = _load_json_rows(path)
            elif path.suffix.lower() == ".jsonl":
                loaded = _load_jsonl_rows(path)
            else:
                loaded = []

            for r in loaded:
                rows.append(r)
                if len(rows) >= max_rows:
                    break

            blob = path.read_bytes()
            provenance.append(
                {
                    "source_name": path.name,
                    "format": fmt,
                    "file_size_bytes": len(blob),
                    "file_sha256": hashlib.sha256(blob).hexdigest(),
                    "rows_loaded": min(len(loaded), max_rows),
                }
            )
            if len(rows) >= max_rows:
                break

        except Exception as e:
            errors.append({"source_name": path.name, "error_type": type(e).__name__, "message": str(e)})

    normalized_rows = _normalize_rows(rows, max_rows)
    schema: List[Dict[str, Any]] = []
    if normalized_rows:
        keys = sorted({k for r in normalized_rows for k in r.keys()})
        for k in keys:
            sample_val = None
            for r in normalized_rows:
                if k in r and r[k] not in (None, ""):
                    sample_val = r[k]
                    break
            schema.append({"name": k, "type": _infer_type(sample_val), "nullable": True})

    return {
        "rows": normalized_rows,
        "row_count": len(normalized_rows),
        "schema": schema,
        "provenance": provenance,
        "errors": errors,
    }


def _sanitize_recipe(raw: Dict[str, Any], *, max_rows: int) -> Dict[str, Any]:
    row_count = raw.get("row_count", max_rows)
    try:
        row_count_i = int(row_count)
    except Exception:
        row_count_i = max_rows
    row_count_i = max(1, min(int(max_rows), row_count_i))

    columns_raw = raw.get("columns")
    cols: List[Dict[str, Any]] = []
    if isinstance(columns_raw, list):
        for c in columns_raw:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or "col")
            typ = str(c.get("type") or "str").lower()
            if typ not in {"int", "float", "str", "bool", "date"}:
                typ = "str"
            cols.append({"name": name, "type": typ, "example": c.get("example")})

    if not cols:
        cols = [
            {"name": "id", "type": "int", "example": 1},
            {"name": "value", "type": "float", "example": 0.5},
            {"name": "category", "type": "str", "example": "A"},
        ]

    return {"row_count": row_count_i, "columns": cols, "schema_version": RECIPE_SCHEMA_VERSION}


class SyntheticRecipeStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "1", "recipes": {}}
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        return {"schema_version": "1", "recipes": {}}

    def get(self, signature_hash: str) -> Optional[Dict[str, Any]]:
        db = self._load()
        recipes = db.get("recipes")
        if not isinstance(recipes, dict):
            return None
        rec = recipes.get(signature_hash)
        return rec if isinstance(rec, dict) else None

    def put(self, signature_hash: str, recipe: Dict[str, Any]) -> None:
        db = self._load()
        recipes = db.get("recipes")
        if not isinstance(recipes, dict):
            recipes = {}
            db["recipes"] = recipes
        recipes[signature_hash] = recipe
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(db, indent=2, default=str), encoding="utf-8")


def _recipe_to_rows(recipe: Dict[str, Any], *, signature_hash: str, seed: int, max_rows: int) -> List[Dict[str, Any]]:
    row_count = min(int(max_rows), int(recipe.get("row_count", max_rows)))
    cols = recipe.get("columns") or []
    if not isinstance(cols, list):
        cols = []

    combined_seed = int(seed) ^ int(signature_hash[:8], 16)
    rng = random.Random(combined_seed)

    rows: List[Dict[str, Any]] = []
    for i in range(max(1, row_count)):
        row: Dict[str, Any] = {}
        for c in cols:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or f"col_{len(row)}")
            typ = str(c.get("type") or "str")
            if typ == "int":
                row[name] = rng.randint(0, 10000)
            elif typ == "float":
                row[name] = round(rng.random() * 1000.0, 6)
            elif typ == "bool":
                row[name] = bool(rng.randint(0, 1))
            elif typ == "date":
                row[name] = f"2024-01-{(i % 28) + 1:02d}"
            else:
                row[name] = f"v_{rng.randint(100, 999)}"
        rows.append(row)
    return rows


def _build_minimized_spec(file_path: Path, code_text: str, orchestrator: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    analyzer = StructuralEnergyAnalyzer()
    profile = analyzer.analyze(file_path, orchestrator=orchestrator)

    metrics = profile.get("metrics") or {}
    signals = profile.get("signals") or {}
    schedule = profile.get("schedule") or {}

    minimized = {
        "orchestrator": orchestrator,
        "file_name": file_path.name,
        "code_sha256": profile.get("code_sha256") or _sha256_text(code_text),
        "task_count": int(metrics.get("total_tasks") or 0),
        "edge_count": int(metrics.get("total_edges") or 0),
        "max_fan_out": int(metrics.get("max_fan_out") or 0),
        "parallelism_index": metrics.get("parallelism_index"),
        "operator_tiers": (signals.get("operator_tiers") or {}),
        "schedule": {
            "enabled": schedule.get("enabled"),
            "runs_per_year_estimate": schedule.get("runs_per_year_estimate"),
            "source": schedule.get("source"),
        },
    }
    return minimized, profile


def _next_mode_for(modes: List[str], idx: int) -> Optional[str]:
    if idx + 1 >= len(modes):
        return None
    return modes[idx + 1]


def _resolve_modes(selected: str) -> List[str]:
    s = (selected or "auto").strip().lower()
    if s == "auto":
        return ["sample", "synthetic", "heuristic"]
    if s == "sample":
        return ["sample", "synthetic", "heuristic"]
    if s == "synthetic":
        return ["synthetic", "heuristic"]
    if s == "heuristic":
        return ["heuristic"]
    return ["sample", "synthetic", "heuristic"]


def _run_runtime_measurement(
    *,
    request: EnergyEvaluationRequest,
    mode_name: str,
    data_source: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    py = Path(request.runtime_python) if request.runtime_python else Path(os.sys.executable)
    input_payload = {
        "orchestrator": request.orchestrator,
        "mode": mode_name,
        "data_source": data_source,
        "workflow_file": str(request.file_path),
        "execution_adapter": (request.execution_adapter or "representative"),
        "max_rows": int(request.max_rows),
        "max_tasks": int(request.max_tasks),
        "seed": int(request.seed),
        "rows": _normalize_rows(rows, request.max_rows),
    }

    with tempfile.TemporaryDirectory(prefix="orcheval_energy_runtime_") as td:
        td_path = Path(td)
        in_path = td_path / "runtime_input.json"
        out_path = td_path / "runtime_output.json"
        in_path.write_text(json.dumps(input_payload, indent=2, default=str), encoding="utf-8")

        payload = run_cli_json(
            python_exe=py,
            module_name="orcheval.energy.runtime_runner_cli",
            args=["--in", str(in_path), "--out", str(out_path)],
            out_json=out_path,
            cwd=request.repo_root,
            env={"PYTHONPATH": str(request.repo_root / "src")},
            timeout_s=int(request.timeout_s),
            stub_type="runtime_energy",
            orchestrator=request.orchestrator,
            file_path=str(request.file_path),
        )
        return payload


def _validate_sample_dataset(dataset: Dict[str, Any], minimized_spec: Dict[str, Any]) -> Tuple[bool, str]:
    row_count = int(dataset.get("row_count") or 0)
    schema = dataset.get("schema") or []
    task_count = int(minimized_spec.get("task_count") or 0)
    if row_count <= 0:
        return False, "sample_dataset_empty"
    if not schema:
        return False, "sample_schema_empty"
    if task_count > 0 and row_count < 1:
        return False, "insufficient_rows_for_workflow"
    return True, "ok"


def evaluate_energy(request: EnergyEvaluationRequest) -> EnergyEvaluationResult:
    started_all = time.time()
    errors: List[Dict[str, Any]] = []
    attempted: List[Dict[str, Any]] = []
    synthetic_info: Optional[Dict[str, Any]] = None
    runtime_measurement: Optional[Dict[str, Any]] = None
    mode_used = "heuristic"
    data_source = "none"

    orch_version = request.orchestrator_version or detect_orchestrator_version(
        request.orchestrator,
        python_exe=request.runtime_python,
    )
    kp_resolution = resolve_knowledge_pack(
        orchestrator=request.orchestrator,
        orchestrator_version=orch_version,
        mode=request.knowledge_pack_mode,
        pack_path=request.knowledge_pack,
        pack_version=request.knowledge_pack_version,
    )

    minimized_spec, heuristic_profile = _build_minimized_spec(
        request.file_path,
        request.code_text,
        request.orchestrator,
    )
    heuristic_rules = EcoDesignRuleEngine(knowledge_pack_resolution=kp_resolution).evaluate(heuristic_profile)
    signature_hash = _sha256_text(json.dumps(minimized_spec, sort_keys=True))

    modes = _resolve_modes(request.mode_selected)

    recipe_store_path = (request.artifacts_dir or request.file_path.parent) / "synthetic_recipes.json"
    recipe_store = SyntheticRecipeStore(recipe_store_path)

    for idx, mode in enumerate(modes):
        mode_start = time.time()
        next_mode = _next_mode_for(modes, idx)

        if mode in {"sample", "synthetic"} and request.orchestrator not in RUNTIME_SUPPORTED:
            attempted.append(
                {
                    "mode": mode,
                    "status": "skipped",
                    "reason": f"runtime_not_supported_for_orchestrator:{request.orchestrator}",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                }
            )
            continue

        if mode == "sample":
            if not request.sample_paths:
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "skipped",
                        "reason": "no_sample_data_provided",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                continue

            dataset = load_sample_dataset(request.sample_paths, max_rows=request.max_rows)
            valid, reason = _validate_sample_dataset(dataset, minimized_spec)
            if not valid:
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "failed",
                        "reason": reason,
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                errors.append({"mode": "sample", "error_type": "SampleValidationFailed", "message": reason})
                continue

            runtime = _run_runtime_measurement(
                request=request,
                mode_name="sample",
                data_source="sample",
                rows=dataset.get("rows") or [],
            )
            if bool(runtime.get("ok", False)):
                mode_used = "sample"
                data_source = "sample"
                runtime_measurement = runtime.get("runtime_measurement") if isinstance(runtime.get("runtime_measurement"), dict) else runtime
                attempted.append(
                    {
                        "mode": "sample",
                        "status": "success",
                        "reason": "ok",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": None,
                        "sample_summary": {
                            "row_count": int(dataset.get("row_count") or 0),
                            "column_count": len(dataset.get("schema") or []),
                            "provenance": dataset.get("provenance") or [],
                            "errors": dataset.get("errors") or [],
                        },
                    }
                )
                break

            attempted.append(
                {
                    "mode": "sample",
                    "status": "failed",
                    "reason": "runtime_measurement_failed",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": next_mode,
                    "subprocess": runtime.get("_subprocess"),
                }
            )
            errors.append(
                {
                    "mode": "sample",
                    "error_type": "RuntimeMeasurementFailed",
                    "message": "runtime measurement failed for sample mode",
                    "details": runtime.get("metadata") or {},
                }
            )
            continue

        if mode == "synthetic":
            if not request.llm_provider or not request.llm_model:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "skipped",
                        "reason": "missing_llm_provider_or_model",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                continue

            try:
                provider = build_provider(
                    request.llm_provider,
                    model=request.llm_model,
                    api_key_env=request.llm_api_key_env,
                    base_url=request.llm_base_url,
                    timeout_s=request.llm_timeout_s,
                )
                ok, err = provider.health_check()
                if not ok:
                    raise LLMProviderError("health_check_failed", err or "provider not ready", provider=request.llm_provider)

                cached = recipe_store.get(signature_hash)
                if cached and isinstance(cached.get("recipe"), dict):
                    recipe = _sanitize_recipe(cached["recipe"], max_rows=request.max_rows)
                    recipe_source = "cache"
                else:
                    raw_recipe = provider.generate_synthetic_spec(minimized_spec, seed=request.seed, max_rows=request.max_rows)
                    recipe = _sanitize_recipe(raw_recipe, max_rows=request.max_rows)
                    recipe_source = "provider"
                    recipe_store.put(
                        signature_hash,
                        {
                            "signature_hash": signature_hash,
                            "provider": provider.provider_name,
                            "model": request.llm_model,
                            "seed": int(request.seed),
                            "template_version": RECIPE_SCHEMA_VERSION,
                            "recipe_hash": _sha256_text(json.dumps(recipe, sort_keys=True)),
                            "recipe": recipe,
                            "updated_at": time.time(),
                        },
                    )

                recipe_hash = _sha256_text(json.dumps(recipe, sort_keys=True))
                rows = _recipe_to_rows(recipe, signature_hash=signature_hash, seed=request.seed, max_rows=request.max_rows)
                synthetic_info = {
                    "provider": request.llm_provider,
                    "model": request.llm_model,
                    "seed": int(request.seed),
                    "recipe_hash": recipe_hash,
                    "signature_hash": signature_hash,
                    "recipe_source": recipe_source,
                    "recipe_schema_version": recipe.get("schema_version"),
                    "row_count": int(recipe.get("row_count") or len(rows)),
                    "column_count": len(recipe.get("columns") or []),
                }

                runtime = _run_runtime_measurement(
                    request=request,
                    mode_name="synthetic",
                    data_source="synthetic",
                    rows=rows,
                )
                if bool(runtime.get("ok", False)):
                    mode_used = "synthetic"
                    data_source = "synthetic"
                    runtime_measurement = runtime.get("runtime_measurement") if isinstance(runtime.get("runtime_measurement"), dict) else runtime
                    attempted.append(
                        {
                            "mode": "synthetic",
                            "status": "success",
                            "reason": "ok",
                            "duration_s": round(time.time() - mode_start, 4),
                            "next_mode": None,
                            "synthetic_info": synthetic_info,
                        }
                    )
                    break

                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": "runtime_measurement_failed",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                        "subprocess": runtime.get("_subprocess"),
                    }
                )
                errors.append(
                    {
                        "mode": "synthetic",
                        "error_type": "RuntimeMeasurementFailed",
                        "message": "runtime measurement failed for synthetic mode",
                        "details": runtime.get("metadata") or {},
                    }
                )
                continue

            except LLMProviderError as e:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": e.code,
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                        "provider_error": e.to_dict(),
                    }
                )
                errors.append({"mode": "synthetic", "error_type": "LLMProviderError", "details": e.to_dict()})
                continue
            except Exception as e:
                attempted.append(
                    {
                        "mode": "synthetic",
                        "status": "failed",
                        "reason": "unexpected_error",
                        "duration_s": round(time.time() - mode_start, 4),
                        "next_mode": next_mode,
                    }
                )
                errors.append({"mode": "synthetic", "error_type": type(e).__name__, "message": str(e)})
                continue

        if mode == "heuristic":
            mode_used = "heuristic"
            data_source = "none"
            runtime_measurement = None
            attempted.append(
                {
                    "mode": "heuristic",
                    "status": "success",
                    "reason": "heuristic_fallback",
                    "duration_s": round(time.time() - mode_start, 4),
                    "next_mode": None,
                }
            )
            break

    heuristic_summary = {
        "metrics": heuristic_profile.get("metrics") or {},
        "signals": {
            "operator_tiers": (heuristic_profile.get("signals") or {}).get("operator_tiers") or {},
            "xcom": (heuristic_profile.get("signals") or {}).get("xcom") or {},
        },
        "rules": {
            "rule_count": heuristic_rules.get("rule_count", 0),
            "rules": heuristic_rules.get("rules", []),
        },
    }

    privacy = {
        "sample_data_persisted": False,
        "raw_rows_persisted": False,
        "llm_payload_mode": "minimized_spec_only",
        "redaction_summary": [
            "raw sample rows are not persisted",
            "runtime payload stores only aggregate metrics",
            "llm prompt includes minimized workflow spec only",
        ],
    }

    metadata = {
        "orchestrator": request.orchestrator,
        "orchestrator_version": orch_version,
        "runtime_supported": request.orchestrator in RUNTIME_SUPPORTED,
        "signature_hash": signature_hash,
        "seed": int(request.seed),
        "max_rows": int(request.max_rows),
        "max_tasks": int(request.max_tasks),
        "execution_adapter_selected": (request.execution_adapter or "representative"),
        "knowledge_pack": kp_resolution.to_dict(),
        "uncertainty": kp_resolution.uncertainty,
        "warnings": resolution_warnings(kp_resolution),
        "total_duration_s": round(time.time() - started_all, 4),
    }

    return EnergyEvaluationResult(
        mode_selected=request.mode_selected,
        mode_used=mode_used,
        data_source=data_source,
        attempted_modes=attempted,
        runtime_measurement=runtime_measurement,
        synthetic_info=synthetic_info,
        privacy=privacy,
        heuristic=heuristic_summary,
        errors=errors,
        metadata=metadata,
    )
