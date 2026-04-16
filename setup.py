from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.egg_info import egg_info as _egg_info
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


PACKAGE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_ROOT
DEFAULT_BUNDLE_ROOT = REPO_ROOT / "postgres-pglite" / "pglite" / "out" / "native"
STAGED_BUNDLE_ROOT = PACKAGE_ROOT / "pglite_native_runtime" / "bundle"
BUILD_SCRIPT = REPO_ROOT / "postgres-pglite" / "build-libpglite.sh"
VERSION = "0.0.1"
BUNDLE_EMPTY_DIRS = PACKAGE_ROOT / "pglite_native_runtime" / "bundle-empty-dirs.txt"


def _bundle_root_from_env() -> Path | None:
    env_bundle = os.environ.get("PGLITE_BUNDLE_PATH")
    if env_bundle:
        return Path(env_bundle).expanduser().resolve()

    env_lib = os.environ.get("PGLITE_LIB_PATH")
    if env_lib:
        lib_path = Path(env_lib).expanduser().resolve()
        if lib_path.exists():
            return lib_path.parent.parent

    return None


def _bundle_library_candidates(bundle_root: Path) -> list[Path]:
    return [
        bundle_root / "lib" / "libpglite.0.dylib",
        bundle_root / "lib" / "libpglite.so.0.1",
    ]


def _bundle_ready(bundle_root: Path) -> bool:
    return any(candidate.exists() for candidate in _bundle_library_candidates(bundle_root))


def _resolve_bundle_source(log_fn) -> Path:
    env_bundle = _bundle_root_from_env()
    if env_bundle is not None and _bundle_ready(env_bundle):
        log_fn(f"using native bundle from {env_bundle}")
        return env_bundle

    if _bundle_ready(DEFAULT_BUNDLE_ROOT):
        log_fn(f"using native bundle from {DEFAULT_BUNDLE_ROOT}")
        return DEFAULT_BUNDLE_ROOT

    if _bundle_ready(STAGED_BUNDLE_ROOT):
        log_fn(f"using pre-staged native bundle from {STAGED_BUNDLE_ROOT}")
        return STAGED_BUNDLE_ROOT

    if not BUILD_SCRIPT.exists():
        raise RuntimeError(
            "could not find a built native bundle and postgres-pglite/build-libpglite.sh is unavailable; "
            "set PGLITE_BUNDLE_PATH or PGLITE_LIB_PATH to an existing bundle"
        )

    log_fn(f"building native bundle with {BUILD_SCRIPT}")
    subprocess.run(["bash", str(BUILD_SCRIPT)], cwd=str(BUILD_SCRIPT.parent), check=True)

    if _bundle_ready(DEFAULT_BUNDLE_ROOT):
        return DEFAULT_BUNDLE_ROOT

    raise RuntimeError(f"native bundle build completed but {DEFAULT_BUNDLE_ROOT} does not contain libpglite")


def _stage_runtime_bundle(log_fn) -> Path:
    def prune_runtime_bundle(bundle_root: Path) -> None:
        sdk_dir = bundle_root / "psycopg2-sdk"
        postgres_bin = bundle_root / "bin" / "postgres"
        if sdk_dir.exists():
            shutil.rmtree(sdk_dir)
            log_fn(f"removed developer-only psycopg2 sdk from {bundle_root}")
        if postgres_bin.exists():
            postgres_bin.unlink()
            log_fn(f"removed unused postgres executable from {bundle_root}")
        bin_dir = postgres_bin.parent
        if bin_dir.exists() and not any(bin_dir.iterdir()):
            bin_dir.rmdir()

    def write_empty_dir_manifest(bundle_root: Path) -> None:
        empty_dirs = sorted(
            str(path.relative_to(bundle_root))
            for path in bundle_root.rglob("*")
            if path.is_dir() and not any(path.iterdir())
        )
        BUNDLE_EMPTY_DIRS.write_text("\n".join(empty_dirs), encoding="utf-8")

    bundle_source = _resolve_bundle_source(log_fn)
    source_resolved = bundle_source.resolve()
    target_resolved = STAGED_BUNDLE_ROOT.resolve()

    if source_resolved == target_resolved:
        prune_runtime_bundle(STAGED_BUNDLE_ROOT)
        write_empty_dir_manifest(STAGED_BUNDLE_ROOT)
        log_fn(f"native bundle already staged in {STAGED_BUNDLE_ROOT}")
        return STAGED_BUNDLE_ROOT

    if STAGED_BUNDLE_ROOT.exists():
        shutil.rmtree(STAGED_BUNDLE_ROOT)
    shutil.copytree(bundle_source, STAGED_BUNDLE_ROOT)
    prune_runtime_bundle(STAGED_BUNDLE_ROOT)
    write_empty_dir_manifest(STAGED_BUNDLE_ROOT)
    log_fn(f"staged native bundle in {STAGED_BUNDLE_ROOT}")
    return STAGED_BUNDLE_ROOT


class build_py(_build_py):
    def run(self) -> None:
        _stage_runtime_bundle(lambda message: self.announce(message, level=2))
        build_bundle = Path(self.build_lib) / "pglite_native_runtime" / "bundle"
        if build_bundle.exists():
            shutil.rmtree(build_bundle)
        super().run()


class egg_info(_egg_info):
    def run(self) -> None:
        _stage_runtime_bundle(lambda message: self.announce(message, level=2))
        super().run()


class bdist_wheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self) -> tuple[str, str, str]:
        _python, _abi, platform = super().get_tag()
        return ("py3", "none", platform)


setup(
    name="pypglite",
    version=VERSION,
    description="PyPGlite: embedded PostgreSQL runtime for Python",
    long_description=(PACKAGE_ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://github.com/Twinbooks/pypglite",
    project_urls={
        "Repository": "https://github.com/Twinbooks/pypglite",
        "Engine": "https://github.com/Twinbooks/postgres-pglite",
        "Upstream PGlite": "https://github.com/electric-sql/pglite",
        "Documentation": "https://github.com/Twinbooks/pypglite#readme",
    },
    python_requires=">=3.10",
    packages=find_packages(
        include=[
            "pypglite",
            "pypglite.*",
            "pglite_native_runtime",
        ]
    ),
    package_data={"pglite_native_runtime": ["bundle-empty-dirs.txt"]},
    include_package_data=True,
    zip_safe=False,
    cmdclass={
        "build_py": build_py,
        "egg_info": egg_info,
        "bdist_wheel": bdist_wheel,
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Database",
    ],
)
