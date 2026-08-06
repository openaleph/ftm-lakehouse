from typer.testing import CliRunner

from ftm_lakehouse.catalog import get_dataset_model
from ftm_lakehouse.cli import cli

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
