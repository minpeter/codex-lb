"""Command-boundary tests; run with --confcutdir=tests/routing (no app runtime)."""

import configparser
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
UNIT = Path(os.environ.get("ROUTE_UNIT", ROOT / "deploy/replica/docker-lan-routes.service"))
PREFLIGHT = ROOT / "deploy/replica/route_preflight.py"
SUBNETS = ("172.17.0.0/16", "172.18.0.0/16", "172.19.0.0/16", "172.28.0.0/16", "10.10.10.0/24")


def unit_command(action: str) -> list[str]:
    config = configparser.ConfigParser(interpolation=None)
    config.read(UNIT)
    return shlex.split(config["Service"][action].replace("$$", "$"))


@pytest.fixture
def commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Fake only external commands, persisting rule mutations like the kernel."""
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "rules": [{"priority": 5199, "src": "all", "dst": net, "table": "main"} for net in SUBNETS],
        "subnet": "172.28.0.0/16", "address": "172.28.0.2", "dev": "br-123456789abc", "fail": "",
    }))
    fake = Path(__file__).with_name("fake_commands.py")
    for name in ("ip", "docker", "systemctl"):
        launcher = tmp_path / name
        launcher.write_text(f"#!{sys.executable}\nexec(compile(open({str(fake)!r}).read(), {str(fake)!r}, 'exec'))\n")
        launcher.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("ROUTE_TEST_STATE", str(state))
    monkeypatch.setenv("ROUTE_TEST_UNIT", str(UNIT))
    return state


def test_start_deduplicates_owned_rules_without_touching_other_owners(commands: Path) -> None:
    # Given duplicate owned rules and same-CIDR rules belonging to other owners.
    state = json.loads(commands.read_text())
    others = [
        {"priority": 5000, "src": "all", "dst": "172.28.0.0/16", "table": "main"},
        {"priority": 5199, "src": "all", "dst": "172.28.0.0/16", "table": "100"},
    ]
    state["rules"] = others + state["rules"] * 2
    commands.write_text(json.dumps(state))
    # When the loaded command starts twice.
    for _ in range(2):
        subprocess.run(unit_command("ExecStart"), check=True, timeout=10)
    # Then only owned duplicates changed, and each owned rule exists once.
    actual = json.loads(commands.read_text())["rules"]
    assert all(rule in actual for rule in others)
    assert len(actual) == len(SUBNETS) + len(others)
    assert all(sum(r["priority"] == 5199 and r["dst"] == net and r["table"] == "main" for r in actual) == 1
               for net in SUBNETS)


def test_stop_is_idempotent_and_preserves_unrelated_same_cidr(commands: Path) -> None:
    # Given an unrelated priority before an owned duplicate.
    state = json.loads(commands.read_text())
    other = {"priority": 5000, "src": "all", "dst": "172.28.0.0/16", "table": "main"}
    state["rules"] = [other] + state["rules"] * 2
    commands.write_text(json.dumps(state))
    # When stopped twice.
    for _ in range(2):
        subprocess.run(unit_command("ExecStop"), check=True, timeout=10)
    # Then only another owner's rule survives.
    assert json.loads(commands.read_text())["rules"] == [other]


@pytest.mark.parametrize("action", ["ExecStart", "ExecStop"])
@pytest.mark.parametrize("failure", ["rule show", "rule del", "rule add"])
def test_unit_propagates_command_failures(commands: Path, action: str, failure: str) -> None:
    # Given an ip command failure. Stop never adds rules.
    if action == "ExecStop" and failure == "rule add":
        failure = "rule del"
    state = json.loads(commands.read_text())
    state["fail"] = failure
    commands.write_text(json.dumps(state))
    # When executing the unit action.
    result = subprocess.run(unit_command(action), capture_output=True, text=True, timeout=10)
    # Then unexpected failures are not masked by a later successful command.
    assert result.returncode == 42


@pytest.mark.parametrize("fault,code", [
    ("healthy", None), ("subnet", "subnet_drift"), ("missing", "owned_rule_count"),
    ("duplicate", "owned_rule_count"), ("route", "wrong_route"), ("unit", "unit_commands"),
    ("inactive", "unit_state"), ("docker", "command_failed"), ("ip", "command_failed"),
    ("systemctl", "command_failed"), ("malformed", "invalid_output"),
])
def test_preflight_cli_reports_fault_without_mutation(commands: Path, fault: str, code: str | None) -> None:
    # Given a captured command boundary snapshot, changed in one dimension.
    state = json.loads(commands.read_text())
    if fault == "subnet":
        state.update(subnet="172.29.0.0/16", address="172.29.0.2")
    if fault == "missing":
        state["rules"].pop()
    if fault == "duplicate":
        state["rules"].append(state["rules"][0])
    if fault == "route":
        state["dev"] = "tailscale0"
    state["fault"] = fault
    commands.write_text(json.dumps(state))
    before = commands.read_bytes()
    # When the real CLI reads command outputs.
    result = subprocess.run([sys.executable, str(PREFLIGHT)], capture_output=True, text=True, timeout=15)
    # Then stdout is machine-readable, errors fail closed, and no state changes.
    report = json.loads(result.stdout)
    assert result.returncode == (0 if code is None else 1), result.stderr
    assert report["ok"] is (code is None)
    assert code is None or code in {issue["code"] for issue in report["issues"]}
    assert commands.read_bytes() == before
