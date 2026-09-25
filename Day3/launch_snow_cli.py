"""Launch Snowflake CLI with the Day 3 movies.csv path as a template variable."""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def snow_file_uri(path: Path) -> str:
    """Return the Snowflake PUT URI form for Windows, macOS, or Linux."""
    return "file://" + path.resolve().as_posix()


def display_command(command: list[str]) -> str:
    if sys.platform == "win32":
        return subprocess.list2cmdline(command)
    try:
        from shlex import join
        return join(command)
    except ImportError:
        return " ".join(command)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch Snowflake CLI with movies_file set for Demo 3."
    )
    parser.add_argument(
        "-c", "--connection", default="training", help="Saved Snowflake CLI connection name."
    )
    parser.add_argument(
        "--print-only", action="store_true", help="Print the command without launching it."
    )
    args = parser.parse_args()

    movies_file = Path(__file__).resolve().parent / "movies.csv"
    if not movies_file.is_file():
        print(f"Required file not found: {movies_file}", file=sys.stderr)
        return 1

    command = [
        "snow", "sql", "-c", args.connection,
        "-D", f"movies_file={snow_file_uri(movies_file)}",
    ]
    print("Starting Snowflake CLI with this command:")
    print(display_command(command))

    if args.print_only:
        return 0
    if shutil.which("snow") is None:
        print("Snowflake CLI was not found. Install it, open a new terminal, and try again.", file=sys.stderr)
        return 1
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
