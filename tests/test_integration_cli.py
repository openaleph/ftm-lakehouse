import csv

import orjson
from ftmq.util import make_entity
from typer.testing import CliRunner

from ftm_lakehouse.catalog import get_dataset_model
from ftm_lakehouse.cli import cli
from ftm_lakehouse.repository.entities.main import EntityRepository
from ftm_lakehouse.repository.factories import clear_caches, get_entities
from tests.conftest import make_test_api
from tests.shared import JANE

runner = CliRunner()


def test_cli(fixtures_path):
    # assert runner.invoke(cli).exit_code == 0  FIXME
    assert runner.invoke(cli, "--help").exit_code == 0
    assert runner.invoke(cli, "--settings").exit_code == 0
    assert runner.invoke(cli, "--version").exit_code == 0

    assert runner.invoke(cli, ["--uri", f"{fixtures_path}/lake"]).exit_code == 0
    assert (
        runner.invoke(
            cli, ["--uri", f"{fixtures_path}/lake", "-d", "test_dataset"]
        ).exit_code
        == 0
    )

    # assert (
    #     runner.invoke(
    #         cli,
    #         [
    #             "--dataset-uri",
    #             f"{fixtures_path}/lake/test_dataset/config.yml",
    #             "-d",
    #             "test_dataset",
    #         ],
    #     ).exit_code
    #     == 0
    # )


def test_cli_configure(tmp_path):
    lake, name = str(tmp_path / "lake"), "cfg_dataset"
    config = tmp_path / "config.yml"
    config.write_text("title: Configured\nsummary: initial\nshards: 4\n")

    # `-c` is required
    assert runner.invoke(cli, ["--uri", lake, "-d", name, "configure"]).exit_code != 0

    res = runner.invoke(
        cli, ["--uri", lake, "-d", name, "configure", "-c", str(config)]
    )
    assert res.exit_code == 0
    model = get_dataset_model(name, f"{lake}/{name}")
    assert model.name == name
    assert model.title == "Configured"
    assert model.shards == 4

    # a partial yaml merges – it doesn't reset unmentioned fields
    partial = tmp_path / "partial.yml"
    partial.write_text("title: Renamed\n")
    res = runner.invoke(
        cli, ["--uri", lake, "-d", name, "configure", "-c", str(partial)]
    )
    assert res.exit_code == 0
    model = get_dataset_model(name, f"{lake}/{name}")
    assert model.title == "Renamed"
    assert model.summary == "initial"
    assert model.shards == 4


def test_cli_statements_sql_api_guard(tmp_path):
    """Raw SQL is local-only: in api mode the command errors before touching
    the store instead of running DuckDB against the http uri.

    No message assert: under ``DEBUG`` anystore's ``ErrorHandler.__exit__``
    re-raises the exception *class*, so the guard's message is lost in tests.
    """
    with make_test_api(tmp_path) as base_url:
        res = runner.invoke(
            cli,
            ["--uri", base_url, "-d", "sql_guard", "statements", "sql", "SELECT 1"],
        )
        assert res.exit_code != 0
        assert isinstance(res.exception, RuntimeError)
        assert "Query took" not in res.output


def test_cli_maintenance_unlock_api_guard(tmp_path):
    """Unlock is local-only: the CLI errors in api mode instead of relying on
    the ``@no_api`` RuntimeError deep in the repository (message assert
    skipped, see above)."""
    with make_test_api(tmp_path) as base_url:
        res = runner.invoke(
            cli,
            ["--uri", base_url, "-d", "unlock_guard", "maintenance", "unlock"],
        )
        assert res.exit_code != 0
        assert isinstance(res.exception, RuntimeError)
        assert "Lock released" not in res.output
        assert "No lock held" not in res.output


def test_cli_entities_iterate_query(tmp_path):
    """``-q`` / ``--rql`` filter the live parquet read; they exclude each other."""
    lake, name = str(tmp_path / "lake"), "query_dataset"
    acme = {
        "id": "acme",
        "schema": "Company",
        "properties": {"name": ["Acme Inc"], "country": ["de"]},
    }
    repo = EntityRepository(name, f"{lake}/{name}")
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(acme))
    repo.flush()

    def ids(*args: str) -> set[str]:
        res = runner.invoke(
            cli, ["--uri", lake, "-d", name, "entities", "iterate", *args]
        )
        assert res.exit_code == 0, res.output
        return {orjson.loads(line)["id"] for line in res.stdout.splitlines() if line}

    assert ids() == {"jane", "acme"}
    assert ids("-q", "filter:schema=Person") == {"jane"}
    assert ids("-q", "filter:group.countries=de") == {"acme"}
    assert ids("--rql", "or(eq(schema,Company),eq(entity_id,jane))") == {"jane", "acme"}

    # the two query surfaces are alternatives, not combinable
    res = runner.invoke(
        cli,
        [
            "--uri",
            lake,
            "-d",
            name,
            "entities",
            "iterate",
            "-q",
            "filter:schema=Person",
            "--rql",
            "eq(entity_id,acme)",
        ],
    )
    assert res.exit_code != 0


def _iterate_statements(uri: str, name: str, out_csv) -> list[dict]:
    res = runner.invoke(
        cli, ["--uri", uri, "-d", name, "statements", "iterate", "-o", str(out_csv)]
    )
    assert res.exit_code == 0, res.output
    with open(out_csv) as fh:
        return list(csv.DictReader(fh))


def test_cli_statements_iterate_local_and_api(tmp_path):
    """``statements iterate`` streams raw row dicts on both backends.

    The command has no api branch – ``query_statements_data`` is overridden
    on the api repository. The two shapes differ by design (local reads the
    physical view, so it carries ``shard`` / ``bucket`` / ``deleted_at``;
    the api reads the wire format), but both carry the statement content and
    the ``fragment`` supersession key that ``statements import`` reads back.
    """
    lake = tmp_path / "lake"
    jane = make_entity(JANE)
    repo = get_entities("iterate_ds", str(lake / "iterate_ds"))
    repo.add(jane, origin="test")
    repo.flush()

    local_rows = _iterate_statements(str(lake), "iterate_ds", tmp_path / "local.csv")
    clear_caches()
    with make_test_api(lake) as base_url:
        api_rows = _iterate_statements(base_url, "iterate_ds", tmp_path / "api.csv")

    common = ["id", "entity_id", "prop", "schema", "value", "origin", "fragment"]
    assert all(c in local_rows[0] for c in common)
    assert all(c in api_rows[0] for c in common)

    def content(rows):
        return sorted(tuple(r[c] for c in common) for r in rows)

    assert content(local_rows) == content(api_rows)
    assert {r["entity_id"] for r in local_rows} == {jane.id}
