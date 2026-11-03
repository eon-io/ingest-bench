# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Wall clock behind a protocol so pacing and scoring can be tested with a fake."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


def now_ms() -> int:
    return SystemClock().now_ms()
