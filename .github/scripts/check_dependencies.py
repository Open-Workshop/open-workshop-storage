"""Check locked runtime and build dependencies against OSV."""
from __future__ import annotations

import json
import re
import sys
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def dependencies():
    cargo = ROOT / "Cargo.lock"
    if cargo.is_file():
        lock = tomllib.loads(cargo.read_text(encoding="utf-8"))
        for package in lock["package"]:
            if package.get("source", "").startswith("registry+"):
                yield "crates.io", package["name"], package["version"]
    requirements = ROOT / "requirements.lock"
    if requirements.is_file():
        for line in requirements.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^([A-Za-z0-9_.-]+)==([^;\s]+)", line)
            if match:
                yield "PyPI", match.group(1), match.group(2)


def main():
    packages = list(dict.fromkeys(dependencies()))
    if not packages:
        raise RuntimeError("No supported lockfile or locked dependencies found")
    findings = []
    for start in range(0, len(packages), 100):
        batch = packages[start : start + 100]
        queries = [
            {"package": {"ecosystem": ecosystem, "name": name}, "version": version}
            for ecosystem, name, version in batch
        ]
        request = urllib.request.Request(
            "https://api.osv.dev/v1/querybatch",
            data=json.dumps({"queries": queries}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            results = json.load(response)["results"]
        if len(results) != len(batch):
            raise RuntimeError("OSV returned an incomplete response")
        for package, result in zip(batch, results):
            for advisory in result.get("vulns", []):
                findings.append((*package, advisory["id"]))
    for ecosystem, name, version, advisory in findings:
        print(f"{ecosystem}: {name} {version}: https://osv.dev/vulnerability/{advisory}")
    print(f"Checked {len(packages)} locked packages; {len(findings)} advisory matches")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
