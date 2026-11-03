# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from pathlib import Path

import pyarrow as pa
import pytest

from ingest_bench import catalog as cat
from ingest_bench import uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.table import cli, create, ddl

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=1MB", "duration_s=2", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=2, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p, seed=2, row_block=64)))


def sqlite_props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": f"file://{tmp_path}/wh"}


def test_parse_partition() -> None:
    assert create.parse_partition("identity(partition_key)") == create.Partition("identity", "partition_key", None)
    assert create.parse_partition("bucket(16, user_id)") == create.Partition("bucket", "user_id", 16)
    assert create.parse_partition("unpartitioned") == create.Partition("unpartitioned", None, None)
    with pytest.raises(ValueError):
        create.parse_partition("days(event_time)")


def test_create_and_drop(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = sqlite_props(tmp_path)
    table = create.create_table(
        props,
        "bench.t1",
        corpus,
        create.parse_partition("identity(partition_key)"),
        {"write.parquet.compression-codec": "zstd"},
    )
    names = [f.name for f in table.schema().fields]
    assert names[:2] == ["id", "partition_key"] and all(f.required for f in table.schema().fields)
    assert table.spec().fields[0].name == "partition_key"
    assert table.properties["write.parquet.compression-codec"] == "zstd"
    rows = pa.table({"id": pa.array([1, 2], pa.int64()), "partition_key": ["p00001", "p00002"]})
    with pytest.raises(ValueError):
        table.append(rows)  # missing columns must be rejected by the schema, proving required=True landed
    create.drop_table(props, "bench.t1")
    create.drop_table(props, "bench.t1")  # idempotent


def test_a_table_location_places_the_namespace_a_warehouse_cannot(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    unusable = {"type": "sql", "uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": "123456789012"}
    location = f"file://{tmp_path}/wh/bench/t_placed"
    table = create.create_table(
        unusable, "bench.t_placed", corpus, create.parse_partition("unpartitioned"), {}, location=location
    )
    assert table.location() == location
    assert cat.open_catalog(unusable).load_namespace_properties("bench")["location"] == f"file://{tmp_path}/wh/bench"

    storage = sqlite_props(tmp_path / "storage")
    (tmp_path / "storage").mkdir()
    create.create_table(
        storage,
        "bench.t_under_warehouse",
        corpus,
        create.parse_partition("unpartitioned"),
        {},
        location=f"file://{tmp_path}/elsewhere/t_under_warehouse",
    )
    assert cat.open_catalog(storage).load_namespace_properties("bench")["location"] == storage["warehouse"] + "/bench"


def test_bucket_and_unpartitioned(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = sqlite_props(tmp_path)
    t = create.create_table(props, "bench.b", corpus, create.parse_partition("bucket(4, user_id)"), {})
    assert "bucket" in str(t.spec().fields[0].transform)
    u = create.create_table(props, "bench.u", corpus, create.parse_partition("unpartitioned"), {})
    assert u.spec().fields == ()  # pyiceberg holds partition fields in a tuple
    with pytest.raises(ValueError, match="not in the corpus schema"):
        create.partition_spec(corpus, create.parse_partition("identity(nope)"))


def test_spark_ddl(corpus: metadata.CorpusMetadata) -> None:
    text = ddl.spark_sql_ddl(
        corpus, "bench.t", create.parse_partition("identity(partition_key)"), {"format-version": "2"}
    )
    assert text.startswith("CREATE TABLE bench.t (")
    assert (
        "id BIGINT NOT NULL" in text
        and "event_time TIMESTAMP_NTZ NOT NULL" in text
        and "payload BINARY NOT NULL" in text
    )
    assert "USING iceberg" in text and "PARTITIONED BY (partition_key)" in text and "'format-version' = '2'" in text


def test_catalog_props_files_then_flags(tmp_path: Path) -> None:
    f = tmp_path / "p.props"
    f.write_text("# comment\nuri=http://a\nwarehouse=s3://w\n")
    props = cat.load_catalog_props(["warehouse=s3://override"], [str(f)])
    assert props == {"uri": "http://a", "warehouse": "s3://override"}
    assert cat.table_identifier("ns.t") == ("ns", "t")
    assert cat.table_identifier("c.ns.t") == ("ns", "t")
    with pytest.raises(ValueError):
        cat.table_identifier("t")


def test_catalog_props_resolve_an_environment_reference(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IB_TEST_CATALOG_TOKEN", "t0ken")
    f = tmp_path / "p.props"
    f.write_text("uri=http://a\ntoken=${env:IB_TEST_CATALOG_TOKEN}\n")
    # A property file may be checked in, so it names the credential rather than
    # holding it, and only the catalog client ever sees the value.
    assert cat.load_catalog_props([], [str(f)])["token"] == "t0ken"
    assert cat.load_catalog_props(["token=${env:IB_TEST_CATALOG_TOKEN}"])["token"] == "t0ken"
    monkeypatch.delenv("IB_TEST_CATALOG_TOKEN")
    with pytest.raises(ValueError, match="IB_TEST_CATALOG_TOKEN"):
        cat.load_catalog_props([], [str(f)])


def test_ddl_only_prints_the_ddl_and_reaches_no_catalog(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, corpus: metadata.CorpusMetadata
) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("--ddl-only must not reach a catalog")

    monkeypatch.setattr(cli, "create_table", refuse)
    code = cli.create(
        [
            "--table",
            "bench.t",
            "--corpus",
            corpus.uri,
            "--partition",
            "identity(partition_key)",
            "--table-prop",
            "format-version=2",
            "--ddl-only",
        ]
    )
    partition = create.parse_partition("identity(partition_key)")
    expected = ddl.spark_sql_ddl(corpus, "bench.t", partition, {"format-version": "2"})
    assert code == 0
    assert capsys.readouterr().out == f"{expected}\n"


def test_table_metadata_prints_the_metadata_location(
    tmp_path: Path, corpus: metadata.CorpusMetadata, capsys: pytest.CaptureFixture[str]
) -> None:
    props = sqlite_props(tmp_path)
    table = create.create_table(props, "bench.m", corpus, create.parse_partition("unpartitioned"), {})
    flags = [flag for key, value in props.items() for flag in ("--catalog-prop", f"{key}={value}")]
    assert cli.metadata_location(["--table", "bench.m", *flags]) == 0
    assert capsys.readouterr().out == f"{table.metadata_location}\n"


def test_table_metadata_reads_a_three_part_name_as_the_same_table(
    tmp_path: Path, corpus: metadata.CorpusMetadata, capsys: pytest.CaptureFixture[str]
) -> None:
    """The catalog client is already scoped to a catalog, so a catalog prefix must
    not become part of the namespace.
    """
    props = sqlite_props(tmp_path)
    table = create.create_table(props, "bench.m", corpus, create.parse_partition("unpartitioned"), {})
    flags = [flag for key, value in props.items() for flag in ("--catalog-prop", f"{key}={value}")]
    assert cli.metadata_location(["--table", "cat.bench.m", *flags]) == 0
    assert capsys.readouterr().out == f"{table.metadata_location}\n"


def test_table_metadata_answers_an_absent_table_with_a_code(
    tmp_path: Path, corpus: metadata.CorpusMetadata, capsys: pytest.CaptureFixture[str]
) -> None:
    props = sqlite_props(tmp_path)
    # One table in the namespace, so what is missing is the table and not the
    # namespace around it.
    create.create_table(props, "bench.present", corpus, create.parse_partition("unpartitioned"), {})
    flags = [flag for key, value in props.items() for flag in ("--catalog-prop", f"{key}={value}")]

    assert cli.metadata_location(["--table", "bench.absent", *flags]) == cli.TABLE_ABSENT
    captured = capsys.readouterr()
    assert captured.out == "", "the location is the only thing this prints on stdout"
    assert "table bench.absent does not exist" in captured.err
