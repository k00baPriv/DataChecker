import subprocess

def run(cmd: list[str]) -> None:
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main() -> None:
    run(["ruff", "check", "--fix", "src/datachecker"])
    run(["ruff", "format", "src/datachecker"])
    print("\nAuto-fix + format complete ✨")

if __name__ == "__main__":
    main()