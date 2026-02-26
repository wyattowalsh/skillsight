from typer.testing import CliRunner

from skillsight.cli import app

runner = CliRunner()


def test_contract_command_lists_paths() -> None:
    result = runner.invoke(app, ["contract"])
    assert result.exit_code == 0
    assert "/v1/skills" in result.stdout
