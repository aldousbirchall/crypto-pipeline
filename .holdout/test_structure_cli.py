"""Structural tests for the CLI entry point (REQ-011).

Verifies the overall CLI structure: subcommand routing, help text formatting,
and flag availability across all commands.
"""

import subprocess
import sys

import pytest


# ---------------------------------------------------------------------------
# CLI structure: all subcommands exist and are routable
# ---------------------------------------------------------------------------

class TestCLIStructure:
    """Verify the CLI has the expected command tree."""

    def test_top_level_help_succeeds(self):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline", "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0

    def test_top_level_no_args_shows_help_or_error(self):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline"],
            capture_output=True, text=True, timeout=15,
        )
        # Should either show help (rc 0) or error with usage (rc != 0)
        output = result.stdout + result.stderr
        assert len(output) > 0, "Running with no args should produce output"

    @pytest.mark.parametrize("cmd", ["backfill", "stream", "query", "refresh"])
    def test_subcommand_listed_in_help(self, cmd):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline", "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert cmd in result.stdout.lower()

    @pytest.mark.parametrize("cmd", ["backfill", "stream", "refresh"])
    def test_subcommand_accepts_help(self, cmd):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline", cmd, "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0

    def test_query_subcommand_has_latest_and_history(self):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline", "query", "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        output = result.stdout.lower()
        assert "latest" in output
        assert "history" in output


# ---------------------------------------------------------------------------
# Flag availability across subcommands
# ---------------------------------------------------------------------------

class TestFlagAvailability:
    """Verify --assets and --db flags appear in help for all subcommands."""

    @pytest.mark.parametrize("cmd_args", [
        ["backfill", "--help"],
        ["stream", "--help"],
        ["refresh", "--help"],
        ["query", "latest", "--help"],
        ["query", "history", "--help"],
    ])
    def test_assets_flag_documented(self, cmd_args):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline"] + cmd_args,
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        output = result.stdout.lower() + result.stderr.lower()
        assert "assets" in output, (
            f"--assets flag not documented for {' '.join(cmd_args)}"
        )

    @pytest.mark.parametrize("cmd_args", [
        ["backfill", "--help"],
        ["stream", "--help"],
        ["refresh", "--help"],
        ["query", "latest", "--help"],
        ["query", "history", "--help"],
    ])
    def test_db_flag_documented(self, cmd_args):
        result = subprocess.run(
            [sys.executable, "-m", "crypto_pipeline"] + cmd_args,
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        output = result.stdout.lower() + result.stderr.lower()
        assert "db" in output, (
            f"--db flag not documented for {' '.join(cmd_args)}"
        )
