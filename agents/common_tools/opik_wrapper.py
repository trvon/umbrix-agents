"""Light-weight, optional integration with Comet OPIK
This helper makes it painless to instrument **Umbrix** LLM/DSPy code with
OPIK traces _without_ introducing a hard dependency.  If the `opik` package is
missing or the user has not provided a `OPIK_API_KEY` the decorator becomes a
no-op and incurs **zero** overhead.

Usage
-----
```
from common_tools.opik_wrapper import trace_llm_call

@trace_llm_call
def call_llm(prompt: str) -> str:
    ...
```
All arguments/keyword arguments are automatically attached to the span as
metadata (with basic redaction for obviously sensitive values such as keys).
"""

from __future__ import annotations

import functools
import os
import types
import typing as _t

_OPIK_AVAILABLE: bool
try:
    # Import lazily so the dependency is optional for environments where OPIK
    # is not enabled.
    import opik  # type: ignore

    _OPIK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover – optional dependency
    opik = types.ModuleType("opik-missing")  # type: ignore
    _OPIK_AVAILABLE = False

# Determine at import-time whether we should emit traces.  This avoids repeated
# environment look-ups in hot paths.
_OPIK_ENABLED: bool = _OPIK_AVAILABLE and bool(os.getenv("OPIK_API_KEY"))

if _OPIK_ENABLED:
    _SESSION = opik.session(  # type: ignore[attr-defined]
        api_key=os.environ["OPIK_API_KEY"],
        app_name=os.getenv("OPIK_APP_NAME", "Umbrix-CTI"),
    )
else:  # pragma: no cover – disabled path
    _SESSION = None  # type: ignore


def _redact(value: _t.Any) -> _t.Any:
    """Basic redaction helper – hides obviously sensitive values."""

    if isinstance(value, str) and ("password" in value.lower() or "api_key" in value.lower()):
        return "***redacted***"
    return value


def trace_llm_call(func: _t.Callable[..., _t.Any]) -> _t.Callable[..., _t.Any]:
    """Decorator that records the wrapped function execution as an OPIK span.

    The decorator is a no-op when OPIK is disabled so it can be applied
    liberally without risk.
    """

    if not _OPIK_ENABLED:
        # Fast-path: return original function unchanged.
        return func

    @functools.wraps(func)
    def _wrapper(*args: _t.Any, **kwargs: _t.Any):  # type: ignore[override]
        span_name = func.__name__

        # We ignore mypy attr error because opik types are dynamic.
        with _SESSION.start_span(span_name) as span:  # type: ignore[attr-defined]
            # Attach arguments (lightly redacted) for easier debugging.
            span.set_attributes(  # type: ignore[attr-defined]
                {
                    "args": [_redact(a) for a in args],
                    "kwargs": {k: _redact(v) for k, v in kwargs.items()},
                }
            )

            try:
                result = func(*args, **kwargs)
                span.set_attributes({"status": "ok"})  # type: ignore[attr-defined]
                return result
            except Exception as exc:
                span.set_attributes({  # type: ignore[attr-defined]
                    "status": "error",
                    "error.type": type(exc).__name__,
                    "error.message": str(exc),
                })
                raise

    return _wrapper  # type: ignore[return-value]
