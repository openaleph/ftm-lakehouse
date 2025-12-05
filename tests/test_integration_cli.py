from typer.testing import CliRunner

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
