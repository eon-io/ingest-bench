# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from ingest_bench import __version__
from ingest_bench.clock import SystemClock, now_ms


def test_version_is_set() -> None:
    assert __version__ == "0.1.0"


def test_now_ms_is_milliseconds() -> None:
    a = now_ms()
    b = SystemClock().now_ms()
    assert 1_600_000_000_000 < a <= b


def test_sleep_ignores_negative() -> None:
    SystemClock().sleep(-1.0)
