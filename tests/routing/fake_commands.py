"""External command fixture for route CLI tests, not an alternate route manager."""

import configparser
import json
import os
import shlex
import sys
from pathlib import Path

state_path = Path(os.environ["ROUTE_TEST_STATE"])
state = json.loads(state_path.read_text())
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
fault = state.get("fault", "")
if fault == tool or (state["fail"] and state["fail"] in " ".join(args)):
    print("injected command failure", file=sys.stderr)
    sys.exit(42)
if fault == "malformed" and tool == "docker":
    print("not-json")
    sys.exit(0)

if tool == "docker":
    if args[0] == "inspect":
        print(json.dumps({"replica_default": {"NetworkID": "123456789abcdef", "IPAddress": state["address"],
                                             "IPPrefixLen": 16}}))
    else:
        print(json.dumps([{"Id": "123456789abcdef", "Driver": "bridge", "Options": {},
                           "IPAM": {"Config": [{"Subnet": state["subnet"]}]}}]))
elif tool == "systemctl":
    config = configparser.ConfigParser(interpolation=None)
    config.read(os.environ["ROUTE_TEST_UNIT"])
    for key in ("ExecStart", "ExecStop"):
        command = " ".join(shlex.split(config["Service"][key]))
        if fault == "unit":
            command = "/bin/true"
        print(f"{key}={{ path=/bin/sh ; argv[]={command} ; ignore_errors=no ; }}")
    print("ActiveState=" + ("inactive" if fault == "inactive" else "active"))
    print("UnitFileState=enabled\nExecMainStatus=0\nNeedDaemonReload=no")
elif tool == "ip":
    if "route" in args:
        print(json.dumps([{"dst": state["address"], "dev": state["dev"], "table": "main"}]))
    elif "-j" in args:
        print(json.dumps(state["rules"]))
    else:
        def matches(rule: dict[str, str | int]) -> bool:
            for flag, field in (("pref", "priority"), ("to", "dst"), ("lookup", "table")):
                if flag in args and str(rule[field]) != args[args.index(flag) + 1]:
                    return False
            return True

        selected = [r for r in state["rules"] if matches(r)]
        if "show" in args:
            for rule in selected:
                print(f"{rule['priority']}: from all to {rule['dst']} lookup {rule['table']}")
        elif "del" in args:
            if not selected:
                sys.exit(2)
            state["rules"].remove(selected[0])
            state_path.write_text(json.dumps(state))
        elif "add" in args:
            state["rules"].append({"priority": int(args[args.index("pref") + 1]), "src": "all",
                                   "dst": args[args.index("to") + 1], "table": args[args.index("lookup") + 1]})
            state_path.write_text(json.dumps(state))
        else:
            sys.exit(3)
else:
    sys.exit(3)
