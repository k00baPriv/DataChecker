import subprocess
import sys

def run(cmd: list[str]) -> None:
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main() -> None:
    run(["ruff", "format", "--check", "src/datachecker"])
    run(["ruff", "check", "src/datachecker"])
    run(["mypy", "src/datachecker"])
    run(["pytest"])
    run(["pip-audit", "--local", "--skip-editable"])
    run(["bandit", "-q", "-r", "src/datachecker"])
    print("\nAll checks passed ✅")
if __name__ == "__main__":
    main()