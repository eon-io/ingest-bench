# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Check tracked files for SPDX headers and unintended private identifiers."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from ingest_bench.collect.privacy import (
    ACCOUNT_DIGITS,
    ACCOUNT_KEY,
    CLOUD_ACCOUNT_ID,
    OBJECT_STORE_URI,
    PLACEHOLDER_ACCOUNT_ID,
    URI_AUTHORITY_PLACEHOLDER,
    captured,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

# Exclude this file because its rules and fixtures contain the identifiers they detect.
SELF = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()

SPDX_LINE = "# SPDX-License-Identifier: CC-BY-NC-SA-4.0"
LICENSED_SUFFIXES = (".py", ".sh")

# Explicitly allow local-stack and test-fixture bucket names. New names require review.
FIXTURE_BUCKETS = frozenset(
    {
        "a-bucket",
        "another-bucket",
        "b",
        "bench",
        "bench-bucket",
        "bucket",
        "corpus",
        "leaked-bucket",
        "override",
        "runs",
        "some-bucket-123456789012",
        "w",
        "warehouse",
    }
)


# A JSON field whose value is a string or an integer, found in the source text
# so duplicate keys and physical lines survive.
JSON_FIELD = re.compile(r'(?P<key>"(?:[^"\\]|\\.)*")\s*:\s*(?P<value>"(?:[^"\\]|\\.)*"|-?\d+(?![.eE\d]))')

# A GCP project ID where the text identifies a project: a resource name that
# continues into a GCP collection, a service-account address, an image
# registry path, or a labelled auto-generated ID. A bare "name-123456" is as
# often a file, part or run name, and "projects/<name>" alone is often a directory.
GCP_PROJECT_ID = re.compile(
    # A resource name starts a token or follows a googleapis.com host. A local directory follows a "/".
    r"(?:(?<![\w/.~-])|googleapis\.com/(?:v\d\w*/)?)"
    r"projects/(?P<path>[a-z][a-z0-9-]{4,28}[a-z0-9])/(?:locations|regions|zones|global|topics|subscriptions|"
    r"datasets|instances|secrets|serviceAccounts|repositories|buckets|jobs)/"
    r"|@(?P<account>[a-z][a-z0-9-]{4,28}[a-z0-9])\.iam\.gserviceaccount\.com"
    r"|\b(?:gcr\.io|[a-z0-9-]+-docker\.pkg\.dev)/(?P<registry>[a-z][a-z0-9-]{4,28}[a-z0-9])/"
    r"|(?i:\bproject(?:[_ -]?id)?\b)[^0-9A-Za-z\n]{0,8}(?P<labelled>[a-z][a-z0-9-]*-\d{6})(?![a-z0-9-])"
)


@dataclass(frozen=True)
class Rule:
    """One shape a leak takes, and the texts of that shape that are allowed."""

    name: str
    pattern: re.Pattern[str]
    remedy: str
    group: int = 0
    allowed: frozenset[str] = frozenset()
    allowed_shape: re.Pattern[str] | None = None
    # Reads the found text from a match whose pattern captures it in one of several groups.
    extract: Callable[[re.Match[str]], str] | None = None

    def permits(self, found: str) -> bool:
        if found in self.allowed:
            return True
        return self.allowed_shape is not None and self.allowed_shape.fullmatch(found) is not None


RULES = (
    Rule(
        name="cloud account id",
        pattern=CLOUD_ACCOUNT_ID,
        remedy=f"use {PLACEHOLDER_ACCOUNT_ID}, the id AWS reserves for documentation",
        allowed=frozenset({PLACEHOLDER_ACCOUNT_ID}),
        extract=captured,
    ),
    Rule(
        name="GCP project id",
        pattern=GCP_PROJECT_ID,
        remedy="use a <project> slot; a project id is one account's and cannot be run by anyone else",
        extract=captured,
    ),
    Rule(
        name="object-store bucket",
        pattern=OBJECT_STORE_URI,
        remedy="use $BUCKET, <bucket> or YOUR_BUCKET, or reuse one of the fixture names",
        extract=captured,
        allowed=FIXTURE_BUCKETS,
        allowed_shape=URI_AUTHORITY_PLACEHOLDER,
    ),
)


@dataclass(frozen=True)
class Leak:
    """A disallowed match and its source line."""

    rule: Rule
    found: str
    line: int


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def leaks_in(text: str, *, is_json: bool = False) -> list[Leak]:
    """Find disallowed identifiers with one-based source line numbers.

    JSON mode rejects malformed documents and checks strings, plus a bare
    string or integer value whose key labels it as an account. Other numbers
    are unclassified, since they cannot be distinguished from metrics.
    """
    if is_json:
        # Tokens preserve duplicate keys and physical source lines despite escaped newlines.
        json.loads(text)
        leaks = [
            Leak(leak.rule, leak.found, line_number(text, match.start()))
            for match in re.finditer(r'"(?:[^"\\]|\\.)*"', text)
            for leak in leaks_in(json.loads(match.group()))
        ]
        # A key such as `account_id` or `glue.id` labels a bare string or integer value as an account.
        for match in JSON_FIELD.finditer(text):
            key, value = json.loads(match.group("key")), str(json.loads(match.group("value")))
            if ACCOUNT_KEY.fullmatch(key) and ACCOUNT_DIGITS.fullmatch(value) and value != PLACEHOLDER_ACCOUNT_ID:
                leaks.append(Leak(RULES[0], value, line_number(text, match.start())))
        return sorted(leaks, key=lambda leak: leak.line)

    found_leaks: list[Leak] = []
    for rule in RULES:
        for match in rule.pattern.finditer(text):
            found = match.group(rule.group) if rule.extract is None else rule.extract(match)
            if not rule.permits(found):
                found_leaks.append(Leak(rule, found, line_number(text, match.start())))
    return found_leaks


def tracked_files() -> tuple[Path, ...]:
    """Use Git's index to exclude untracked local config, run artifacts and virtualenvs."""
    listing = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return tuple(REPO_ROOT / name for name in listing.split("\0") if name)


def scan_files(paths: tuple[Path, ...], *, root: Path) -> list[str]:
    """Report leaks and JSON parse errors without stopping at a malformed file."""
    reported: list[str] = []
    for path in paths:
        name = path.relative_to(root).as_posix()
        if name == SELF:
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        is_json = path.suffix in {".json", ".jsonl"}
        # JSON strings may contain Unicode separators that splitlines() treats as line endings.
        records = (
            [(number, line) for number, line in enumerate(text.split("\n"), start=1) if line.strip()]
            if path.suffix == ".jsonl"
            else [(1, text)]
        )
        for start_line, record in records:
            try:
                leaks = leaks_in(record, is_json=is_json)
            except json.JSONDecodeError as error:
                reported.append(f"{name}:{start_line + error.lineno - 1}: invalid JSON: {error.msg}")
                continue
            for leak in leaks:
                line = start_line + leak.line - 1
                reported.append(f"{name}:{line}: {leak.rule.name} {leak.found!r} — {leak.rule.remedy}")
    return reported


def test_the_sweep_reads_the_whole_tracked_tree() -> None:
    names = {path.relative_to(REPO_ROOT).as_posix() for path in tracked_files()}
    assert len(names) >= 150, f"the tracked listing collapsed to {len(names)} files"
    for expected in (
        ".github/workflows/ci.yml",
        "Dockerfile",
        "README.md",
        "deploy/aws/iam/harness-policy.json",
        "deploy/k8s/harness-job.yaml.tmpl",
        "docs/examples/external-flink/job.sql",
        "docs/examples/smoke-flink/summary.json",
        "docs/results-format.md",
        "engines/flink/sql-runner/pom.xml",
        "pyproject.toml",
        "results/RESULTS.md",
        "specs/smoke/smoke-flink.yaml",
        "scripts/smoke.sh",
        "site.example.yaml",
        "workloads/schemas/events.json",
    ):
        assert expected in names, f"the walk did not reach {expected}"


def test_every_script_and_module_declares_its_licence() -> None:
    """Keep a shebang first when present; the SPDX line follows it."""
    missing: list[str] = []
    for path in tracked_files():
        if path.suffix not in LICENSED_SUFFIXES:
            continue
        lines = path.read_text().splitlines()
        head = lines[1:2] if lines[:1] and lines[0].startswith("#!") else lines[:1]
        if head != [SPDX_LINE]:
            missing.append(path.relative_to(REPO_ROOT).as_posix())
    assert not missing, f"{len(missing)} file(s) do not open with {SPDX_LINE!r}:\n" + "\n".join(sorted(missing))


def test_nothing_in_the_tree_names_where_it_came_from() -> None:
    reported = scan_files(tracked_files(), root=REPO_ROOT)
    assert not reported, "tracked file scan failed:\n" + "\n".join(reported)


@dataclass(frozen=True)
class Sample:
    """Matcher input and expected rule/value pairs."""

    name: str
    text: str
    expected: tuple[tuple[str, str], ...]
    is_json: bool = False


# Exercise each rule independently so a clean tree cannot hide a broken matcher.
# SELF excludes these leak fixtures from the tracked file scan.
LEAK_SAMPLES = (
    Sample(
        name="JSON numeric metrics are not account ids",
        text='{"geometry": {"live": {"bytes": 347645076761}}, '
        '"counts": [581832048035, -197508149223, 347645076761.5, 347645076761e2]}',
        expected=(),
        is_json=True,
    ),
    Sample(
        name="JSON string values still contain account ids in context",
        text='{"bytes": 347645076761, "sizes": ["987654321098"], '
        '"registries": ["987654321098.dkr.ecr.eu-west-1.amazonaws.com"]}',
        expected=(("cloud account id", "987654321098"),),
        is_json=True,
    ),
    Sample(
        name="JSON keys still contain account ids",
        text='{"nested": [{"arn:aws:iam::987654321098:root": 347645076761}]}',
        expected=(("cloud account id", "987654321098"),),
        is_json=True,
    ),
    Sample(
        name="JSON escapes cannot hide account ids",
        text=r'{"role": "arn:aws:iam::\u003987654321098:role/r"}',
        expected=(("cloud account id", "987654321098"),),
        is_json=True,
    ),
    Sample(
        name="bare 12-digit measurements are not account ids",
        text="Kafka held 360000000000 B, the topic reached offset 987654321098, and rows: 581832048035.",
        expected=(),
    ),
    Sample(
        name="an account id that the text labels",
        text="Deploy into AWS account 987654321098, or set AWS_ACCOUNT_ID=987654321099.",
        expected=(("cloud account id", "987654321098"), ("cloud account id", "987654321099")),
    ),
    Sample(
        name="an account id in an AWSLogs path, and a number that only prefixes a name",
        text="Logs land under AWSLogs/987654321098/ and in the 987654321099-lake bucket.",
        expected=(("cloud account id", "987654321098"),),
    ),
    Sample(
        name="numbers joined to names are not account ids",
        text="Files bytes-360000000000, snap-987654321098.avro and events-202609251234.json; accounted 360000000000.",
        expected=(),
    ),
    Sample(
        name="bucket URLs over HTTPS",
        text="Read https://private-lake.s3.us-east-1.amazonaws.com/x, https://s3.eu-west-1.amazonaws.com/other-lake/y "
        "and https://storage.googleapis.com/gcs-lake/z.",
        expected=(
            ("object-store bucket", "private-lake"),
            ("object-store bucket", "other-lake"),
            ("object-store bucket", "gcs-lake"),
        ),
    ),
    Sample(
        name="an environment reference is a bucket placeholder",
        text="Stage into s3://${env:BUCKET}/corpus.",
        expected=(),
    ),
    Sample(
        name="a literal suffix after an environment reference is a bucket",
        text="Stage into s3://${env:BUCKET}-private/corpus.",
        expected=(("object-store bucket", "${env:BUCKET}-private"),),
    ),
    Sample(
        name="more bucket URL forms",
        text="https://gcs-host-lake.storage.googleapis.com/d, https://s3.us-east-1.amazonaws.com:443/port-lake/d "
        "and https://storage.googleapis.com/storage/v1/b/api-lake/o.",
        expected=(
            ("object-store bucket", "gcs-host-lake"),
            ("object-store bucket", "port-lake"),
            ("object-store bucket", "api-lake"),
        ),
    ),
    Sample(
        name="local project directories are not GCP projects",
        text="Clone into ~/projects/lakehouse-ingest-bench or file:///home/runner/projects/my-project/build, "
        "see file:///home/runner/projects/lakehouse-ingest-bench/jobs/producer.py and projects/my-notes/locations.md.",
        expected=(),
    ),
    Sample(
        name="a GCP resource name behind an API host",
        text="POST https://pubsub.googleapis.com/v1/projects/data-lake-prod/topics/bench:publish",
        expected=(("GCP project id", "data-lake-prod"),),
    ),
    Sample(
        name="GCS JSON API hosts and prefixes",
        text="https://www.googleapis.com/storage/v1/b/www-lake/o, "
        "https://content-storage.googleapis.com/download/storage/v1/b/content-lake/o/part, "
        "https://storage.googleapis.com/download/storage/v1/b/dl-lake/o/part?alt=media.",
        expected=(
            ("object-store bucket", "www-lake"),
            ("object-store bucket", "content-lake"),
            ("object-store bucket", "dl-lake"),
        ),
    ),
    Sample(
        name="JSON fields that label an account",
        text='{"account_id": "987654321098", "glue.id": 987654321099, "bytes": 987654321097}',
        expected=(("cloud account id", "987654321098"), ("cloud account id", "987654321099")),
        is_json=True,
    ),
    Sample(
        name="GCP project ids in context",
        text="Use projects/data-lake-prod/locations/us, sa@data-lake-prod.iam.gserviceaccount.com and "
        "us-docker.pkg.dev/data-lake-prod/bench/harness:1.",
        expected=(
            ("GCP project id", "data-lake-prod"),
            ("GCP project id", "data-lake-prod"),
            ("GCP project id", "data-lake-prod"),
        ),
    ),
    Sample(
        name="names that end in six digits are not project ids",
        text="Write part-000001.parquet for benchmark-100000 under abc-123456-def0.",
        expected=(),
    ),
    Sample(
        name="JSON metrics do not hide other leaks",
        text='{"bytes": 347645076761, "notes": ["Eon", "someone@example.com", "s3://acme-prod-lake/data"]}',
        expected=(("object-store bucket", "acme-prod-lake"),),
        is_json=True,
    ),
    Sample(
        name="a decimal fraction is not an account id",
        text='"rows_per_s": 19469.653333333332',
        expected=(),
    ),
    Sample(
        name="account id following a nonnumeric dot",
        text="account.987654321098",
        expected=(("cloud account id", "987654321098"),),
    ),
    Sample(
        name="vocabulary as whole words",
        text="Run the adevents corpus on the Eon platform during the rise campaign with maelstrom.",
        expected=(),
    ),
    Sample(
        name="vocabulary inside longer words",
        text="A peony blooms once an aeon; Napoleon watched the sunrise, and prices have arisen since.",
        expected=(),
    ),
    Sample(
        name="the domain",
        text="Further reading lives at https://docs.eon.io/lakehouse.",
        expected=(),
    ),
    Sample(
        name="external engine display names",
        text="Compared against ExampleEngine, Example Engine and another external writer.",
        expected=(),
    ),
    Sample(
        name="the word native on its own",
        text="The image is multi-arch and runs natively; the operator's native mode sizes the fleet.",
        expected=(),
    ),
    Sample(
        name="account id in an ARN",
        text="arn:aws:kafka:eu-west-1:987654321098:cluster/a-cluster/aaaa-bbbb-1",
        expected=(("cloud account id", "987654321098"),),
    ),
    Sample(
        name="account id in a registry host",
        text="987654321098.dkr.ecr.eu-west-1.amazonaws.com/lakehouse-ingest-bench/harness:abc1234",
        expected=(("cloud account id", "987654321098"),),
    ),
    Sample(
        name="account id as a bucket suffix",
        text="The corpus is at s3://lake-987654321098/corpus.",
        expected=(("object-store bucket", "lake-987654321098"),),
    ),
    Sample(
        name="account id behind an underscore",
        text="Credentials are cached at /var/run/acct_987654321098.json.",
        expected=(("cloud account id", "987654321098"),),
    ),
    Sample(
        name="millisecond epochs and a digest",
        text="at_ms 1757440012345, epoch_ms 1757440000000, sha256:ab987654321098cd, and 123456789012.",
        expected=(),
    ),
    Sample(
        name="a project id",
        text="Point the drivers at project my-data-lake-483921 in us-central1.",
        expected=(("GCP project id", "my-data-lake-483921"),),
    ),
    Sample(
        name="run ids and dates that end in digits",
        text="run_id aws-smoke-spark-20260909T193155Z, corpus smoke-1a2b3c4d, recorded 2026-09-09.",
        expected=(),
    ),
    Sample(
        name="an address",
        text="Ask someone@example.com for the credentials.",
        expected=(),
    ),
    Sample(
        name="an address in a commit trailer",
        text="Co-Authored-By: Claude <noreply@anthropic.com>",
        expected=(),
    ),
    Sample(
        name="real-looking bucket roots",
        text="Stage into s3://acme-prod-lake/corpus and read back from gs://analytics-raw-eu/runs.",
        expected=(
            ("object-store bucket", "acme-prod-lake"),
            ("object-store bucket", "analytics-raw-eu"),
        ),
    ),
    Sample(
        name="bucket placeholders and fixtures",
        text="s3://YOUR_BUCKET/corpus, s3://$BUCKET/runs, gs://<corpus_root>/x, s3://b/runs, s3://warehouse/",
        expected=(),
    ),
)


@pytest.mark.parametrize("sample", LEAK_SAMPLES, ids=[sample.name for sample in LEAK_SAMPLES])
def test_the_matcher_reads_a_leak_and_leaves_its_lookalike(sample: Sample) -> None:
    found = sorted((leak.rule.name, leak.found) for leak in leaks_in(sample.text, is_json=sample.is_json))
    assert found == sorted(sample.expected)


def test_json_scan_preserves_source_lines_and_duplicate_keys() -> None:
    text = '{\n  "role": "prefix\\narn:aws:iam::987654321098:root",\n  "role": "arn:aws:iam::987654321099:root"\n}'
    assert [(leak.rule.name, leak.found, leak.line) for leak in leaks_in(text, is_json=True)] == [
        ("cloud account id", "987654321098", 2),
        ("cloud account id", "987654321099", 3),
    ]


def test_json_scan_rejects_malformed_documents() -> None:
    with pytest.raises(json.JSONDecodeError):
        leaks_in('{"bytes": 347645076761', is_json=True)


@pytest.mark.parametrize("suffix", [".json", ".jsonl"])
def test_tree_scan_reports_malformed_json_and_continues(tmp_path: Path, suffix: str) -> None:
    malformed = tmp_path / f"malformed{suffix}"
    malformed.write_text('{"bytes": 347645076761}\n{"bytes": }\n')
    later = tmp_path / "later.json"
    later.write_text('{"role": "arn:aws:iam::987654321098:root"}')

    report = "\n".join(scan_files((malformed, later), root=tmp_path))
    assert f"malformed{suffix}:2: invalid JSON:" in report
    assert "later.json:1: cloud account id '987654321098'" in report


@pytest.mark.parametrize(
    ("record", "leaked_id"),
    [
        ('{"nested": [{"bytes": 581832048035}]}', None),
        ('{"role": "arn:aws:iam::987654321098:root"}', "987654321098"),
        ('{"arn:aws:iam::987654321099:root": 581832048035}', "987654321099"),
    ],
)
def test_tree_scan_treats_jsonl_records_as_json(tmp_path: Path, record: str, leaked_id: str | None) -> None:
    path = tmp_path / "metrics.jsonl"
    path.write_text('{"bytes": 347645076761}\n' + record + "\n")

    report = "\n".join(scan_files((path,), root=tmp_path))
    if leaked_id is None:
        assert not report
    else:
        assert f"metrics.jsonl:2: cloud account id '{leaked_id}'" in report
        assert "347645076761" not in report
        assert "581832048035" not in report


@pytest.mark.parametrize(
    "text",
    [
        '{"a": 1}\n\n{"b": 2}\n',
        '{"a": 1}\n\n',
        '{"a": 1}\n \t \n{"b": 2}\n',
    ],
)
def test_jsonl_scan_ignores_blank_records(tmp_path: Path, text: str) -> None:
    path = tmp_path / "blank.jsonl"
    path.write_text(text)
    assert scan_files((path,), root=tmp_path) == []


def test_jsonl_scan_counts_blank_lines_in_diagnostics(tmp_path: Path) -> None:
    path = tmp_path / "blank.jsonl"
    path.write_text('\n{"bytes": 347645076761}\n \t \n{"role": "arn:aws:iam::987654321098:root"}\n\n')
    reported = scan_files((path,), root=tmp_path)
    assert len(reported) == 1
    assert reported[0].startswith("blank.jsonl:4: cloud account id '987654321098'")


@pytest.mark.parametrize("separator", ["\u0085", "\u2028", "\u2029"])
def test_jsonl_scan_preserves_unicode_separators_in_strings(tmp_path: Path, separator: str) -> None:
    path = tmp_path / "separator.jsonl"
    path.write_text('{"note": "a' + separator + 'b"}\n{"role": "arn:aws:iam::987654321098:root"}\n')
    reported = scan_files((path,), root=tmp_path)
    assert len(reported) == 1
    assert reported[0].startswith("separator.jsonl:2: cloud account id '987654321098'")


@pytest.mark.parametrize("control", ["\v", "\f"])
def test_jsonl_scan_rejects_control_characters_without_shifting_lines(tmp_path: Path, control: str) -> None:
    path = tmp_path / "control.jsonl"
    path.write_text('{"note": "a' + control + 'b"}\n{"role": "arn:aws:iam::987654321098:root"}\n')
    reported = scan_files((path,), root=tmp_path)
    assert len(reported) == 2
    assert reported[0].startswith("control.jsonl:1: invalid JSON:")
    assert reported[1].startswith("control.jsonl:2: cloud account id '987654321098'")


def test_every_rule_is_exercised_by_a_sample() -> None:
    covered = {rule_name for sample in LEAK_SAMPLES for rule_name, _ in sample.expected}
    assert covered == {rule.name for rule in RULES}
