"""Exercise SQLite initialization/leases from a built wheel, not the checkout."""

import argparse
import pathlib
import subprocess
import sys
import sysconfig
import tempfile
import zipfile


PROBE = """
import asyncio
import pathlib
import sys
sys.path.insert(0, sys.argv[3])  # Dependencies, without processing editable .pth hooks.
sys.path.insert(0, sys.argv[1])
import llmproxy.db
assert pathlib.Path(llmproxy.db.__file__).is_relative_to(pathlib.Path(sys.argv[1]))
from llmproxy.db import SqliteDatabase

async def main():
    db = await SqliteDatabase.create('sqlite://' + sys.argv[2])
    try:
        assert await db.admission_acquire('wheel-probe', 'owner', 1, 30)
        assert not await db.admission_acquire('wheel-probe', 'other', 1, 30)
        assert await db.admission_release('wheel-probe', 'owner')
    finally:
        await db.close()
asyncio.run(main())
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=pathlib.Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    with tempfile.TemporaryDirectory() as directory:
        installed = pathlib.Path(directory) / "installed"
        with zipfile.ZipFile(wheel) as archive:
            archive.extractall(installed)
        result = subprocess.run([sys.executable, "-I", "-S", "-c", PROBE,
            str(installed), str(pathlib.Path(directory) / "state.sqlite"),
            sysconfig.get_paths()["purelib"]], cwd=directory, check=False, timeout=15)
    if result.returncode:
        raise SystemExit(result.returncode)
    print("Wheel SQLite schema/ownership smoke PASS (isolated from checkout)")


if __name__ == "__main__":
    main()
