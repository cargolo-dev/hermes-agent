"""CARGOLO ASR operations package.

Implements a production-oriented MVP for:
- normalized webhook email intake
- per-order case folders under Hermes home
- deterministic normalization, delta comparison, and draft/task suggestions
- mockable mail-history and TMS/task adapters
- daily operational reporting
"""

from .models import IncomingEmailEvent, ProcessingResult
from .processor import process_email_event
from .reporting import generate_daily_report

__all__ = [
    "IncomingEmailEvent",
    "ProcessingResult",
    "process_email_event",
    "generate_daily_report",
]
