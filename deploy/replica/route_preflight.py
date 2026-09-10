#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pydantic>=2.12,<3"]
# ///
"""Read-only host check: python3 deploy/replica/route_preflight.py [container]."""

from __future__ import annotations

import configparser
import ipaddress
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Final, TypedDict

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

SUBNETS: Final = ("172.17.0.0/16", "172.18.0.0/16", "172.19.0.0/16", "172.28.0.0/16", "10.10.10.0/24")
B_SUBNET: Final = ipaddress.IPv4Network("172.28.0.0/16")
UNIT: Final = "docker-lan-routes.service"


class Issue(TypedDict):
    code: str
    detail: str


class Attachment(BaseModel):
    model_config = ConfigDict(frozen=True)
    network_id: str = Field(alias="NetworkID")
    address: ipaddress.IPv4Address = Field(alias="IPAddress")
    prefix: int = Field(alias="IPPrefixLen", ge=0, le=32)


class Subnet(BaseModel):
    model_config = ConfigDict(frozen=True)
    subnet: ipaddress.IPv4Network | ipaddress.IPv6Network = Field(alias="Subnet")


class IPAM(BaseModel):
    model_config = ConfigDict(frozen=True)
    config: list[Subnet] = Field(alias="Config")


class Network(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str = Field(alias="Id", min_length=12)
    driver: str = Field(alias="Driver")
    options: dict[str, str] = Field(alias="Options")
    ipam: IPAM = Field(alias="IPAM")


class Rule(BaseModel):
    model_config = ConfigDict(frozen=True)
    priority: int
    src: str = "all"
    dst: str = "0.0.0.0/0"
    dstlen: int | None = None
    table: str | int = ""
    fwmark: str | None = None
    iif: str | None = None
    oif: str | None = None
    inverted: bool = Field(default=False, alias="not")
    ipproto: str | None = None
    sport: str | None = None
    dport: str | None = None
    uidrange: str | None = None


class Route(BaseModel):
    model_config = ConfigDict(frozen=True)
    dev: str = ""
    table: str | int = "main"


class CommandFailure(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


def run(*args: str) -> str:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired as exc:
        raise CommandFailure("command_timeout", shlex.join(args)) from exc
    if result.returncode:
        raise CommandFailure("command_failed", f"{shlex.join(args)}: exit {result.returncode}: {result.stderr.strip()}")
    return result.stdout


def check(container: str) -> list[Issue]:
    """Inspect only the selected replica, not every Docker/private subnet."""
    issues: list[Issue] = []
    attachments = TypeAdapter(dict[str, Attachment]).validate_json(
        run("docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", container))
    if len(attachments) != 1:
        return [{"code": "network_attachment", "detail": "Expected exactly one replica network attachment"}]
    attachment = next(iter(attachments.values()))
    address = attachment.address
    attached_subnet = ipaddress.IPv4Network(f"{address}/{attachment.prefix}", strict=False)
    network, = TypeAdapter(list[Network]).validate_json(run("docker", "network", "inspect", attachment.network_id))
    subnets = [item.subnet for item in network.ipam.config]
    ipv4_subnets = [net for net in subnets if net.version == 4]
    if attached_subnet != B_SUBNET or ipv4_subnets != [B_SUBNET] or address not in B_SUBNET:
        issues.append({"code": "subnet_drift", "detail": f"attachment={attached_subnet}; IPAM={ipv4_subnets}"})
    bridge = network.options.get("com.docker.network.bridge.name", "br-" + network.id[:12])
    if network.driver != "bridge":
        issues.append({"code": "network_driver", "detail": str(network.driver)})

    properties = dict(line.split("=", 1) for line in run(
        "systemctl", "show", UNIT,
        "--property=ExecStart,ExecStop,ActiveState,UnitFileState,ExecMainStatus,NeedDaemonReload"
    ).splitlines())
    expected = configparser.ConfigParser(interpolation=None)
    with Path(__file__).with_name(UNIT).open() as source:
        expected.read_file(source)
    for action in ("ExecStart", "ExecStop"):
        # systemctl's argv[] rendering loses shell quote delimiters, not argument contents.
        wanted = " ".join(shlex.split(expected["Service"][action])).replace("$$", "$")
        loaded = re.search(r"argv\[\]=(.*?) ; ignore_errors=(yes|no)", properties[action])
        if loaded is None or loaded[1].replace("$$", "$") != wanted or loaded[2] != "no":
            issues.append({"code": "unit_commands", "detail": action})
    for key, value in (("ActiveState", "active"), ("UnitFileState", "enabled"),
                       ("ExecMainStatus", "0"), ("NeedDaemonReload", "no")):
        if properties[key] != value:
            issues.append({"code": "unit_state", "detail": f"{key}={properties[key]}"})

    rules = TypeAdapter(list[Rule]).validate_json(run("ip", "-j", "-4", "rule", "show"))
    for subnet in SUBNETS:
        count = 0
        for rule in rules:
            destination = rule.dst
            if rule.dstlen is not None and "/" not in destination:
                destination += "/" + str(rule.dstlen)
            if (rule.priority == 5199 and rule.table in ("main", 254)
                    and ipaddress.ip_network(destination) == ipaddress.ip_network(subnet)):
                count += 1
                if rule.src != "all" or rule.inverted or any(value is not None for value in (
                    rule.fwmark, rule.iif, rule.oif, rule.ipproto, rule.sport, rule.dport, rule.uidrange
                )):
                    issues.append({"code": "owned_rule_selector", "detail": subnet})
        if count != 1:
            issues.append({"code": "owned_rule_count", "detail": f"{subnet}: {count}"})

    routes = TypeAdapter(list[Route]).validate_json(run("ip", "-j", "-4", "route", "get", str(address)))
    if len(routes) != 1 or routes[0].dev != bridge or routes[0].table not in ("main", 254):
        issues.append({"code": "wrong_route", "detail": json.dumps([route.model_dump() for route in routes])})
    return issues


def main() -> int:
    try:
        if len(sys.argv) > 2:
            issues: list[Issue] = [{"code": "usage", "detail": "route_preflight.py [container]"}]
        else:
            issues = check(sys.argv[1] if len(sys.argv) == 2 else "codex-lb-b")
    except CommandFailure as exc:
        issues = [{"code": exc.code, "detail": str(exc)}]
    except OSError as exc:
        issues = [{"code": "command_failed", "detail": str(exc)}]
    except (ValueError, KeyError, TypeError, AttributeError, configparser.Error) as exc:
        issues = [{"code": "invalid_output", "detail": str(exc)}]
    print(json.dumps({"ok": not issues, "issues": issues}, sort_keys=True))
    return int(bool(issues))


if __name__ == "__main__":
    sys.exit(main())
