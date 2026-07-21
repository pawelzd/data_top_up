"""Unified structured logging — VENDORED COPY of rl-crypto/deploy/obs_log.py.

One JSON object per line to stdout so Cloud Logging captures it as a structured
`jsonPayload`. Every event shares the envelope {ts, service, event, level,
cycle_ts?, ...} so a single query spans every service/job in the system. Kept
byte-compatible with the canonical module (stdlib only — no third-party deps).
If you change the envelope, change it in rl-crypto/deploy/obs_log.py too.
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import sys
from typing import Any, Optional


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _jsonable(v: Any) -> Any:
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def log_event(service: str, event: str, *, level: str = "INFO",
              cycle_ts: Optional[str] = None, stream=None, **fields: Any) -> dict:
    rec: dict[str, Any] = {"ts": _now_iso(), "service": service,
                           "event": event, "level": level}
    if cycle_ts:
        rec["cycle_ts"] = cycle_ts
    for k, v in fields.items():
        if v is not None:
            rec[k] = _jsonable(v)
    out = stream or sys.stdout
    out.write(json.dumps(rec, default=str, separators=(",", ":"), allow_nan=False) + "\n")
    out.flush()
    return rec


class Logger:
    def __init__(self, service: str, cycle_ts: Optional[str] = None, stream=None,
                 _ctx: Optional[dict] = None):
        self.service = service
        self.cycle_ts = cycle_ts
        self.stream = stream
        self._ctx = dict(_ctx or {})

    def bind(self, **ctx: Any) -> "Logger":
        return Logger(self.service, self.cycle_ts, self.stream, {**self._ctx, **ctx})

    def event(self, event: str, *, level: str = "INFO", **fields: Any) -> dict:
        return log_event(self.service, event, level=level, cycle_ts=self.cycle_ts,
                         stream=self.stream, **{**self._ctx, **fields})

    def info(self, event: str, **fields: Any) -> dict:
        return self.event(event, level="INFO", **fields)

    def warn(self, event: str, **fields: Any) -> dict:
        return self.event(event, level="WARNING", **fields)

    def error(self, event: str, **fields: Any) -> dict:
        return self.event(event, level="ERROR", **fields)
