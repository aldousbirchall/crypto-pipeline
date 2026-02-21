"""REQ-011: CLI Entry Point.

Tests that the crypto-pipeline CLI provides the expected subcommands
and help output.
"""

import subprocess
import sys

import pytest


# ---------------------------------------------------------------------------
# AC-1: --help lists all four subcommands
# ---------------------------------------------------------------------------

def test_help_lists_subcommands():
    """AC-1: crypto-pipeline --help lists backfill, stream, query, refresh."""
    result = subprocess.run(
        [sys.executable, "-m", "crypto_pipeline", "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout.lower()
    for cmd in ["backfill", "stream", "query", "refresh"]:
        assert cmd in output, f"Subcommand '{cmd}' not found in --help output"


# ---------------------------------------------------------------------------
# AC-2: Invalid subcommand exits non-zero
# ---------------------------------------------------------------------------

def test_invalid_subcommand_exits_nonzero():
    """AC-2: An invalid subcommand prints an error and exits non-zero."""
    result = subprocess.run(
        [sys.executable, "-m", "crypto_pipeline", "nonexistent_cmd"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode != 0, (
        f"Expected non-zero exit for invalid subcommand. "
        f"stdout: {result.stdout}, stderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# AC-3: query --help lists latest and history sub-subcommands
# ---------------------------------------------------------------------------

def test_query_help_lists_subsubcommands():
    """AC-3: crypto-pipeline query --help lists 'latest' and 'history'."""
    result = subprocess.run(
        [sys.executable, "-m", "crypto_pipeline", "query", "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout.lower()
    assert "latest" in output, "'latest' not found in query --help output"
    assert "history" in output, "'history' not found in query --help output"


# ---------------------------------------------------------------------------
# Additional: Module is importable
# ---------------------------------------------------------------------------

def test_module_importable():
    """The crypto_pipeline package can be imported."""
    result = subprocess.run(
        [sys.executable, "-c", "import crypto_pipeline"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, (
        f"crypto_pipeline not importable. stderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# Additional: Each subcommand accepts --help
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("subcommand", ["backfill", "stream", "refresh"])
def test_subcommand_help(subcommand):
    """Each subcommand accepts --help without error."""
    result = subprocess.run(
        [sys.executable, "-m", "crypto_pipeline", subcommand, "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, (
        f"{subcommand} --help failed. stderr: {result.stderr}"
    )
