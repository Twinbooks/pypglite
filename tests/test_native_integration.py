from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pypglite._native import _LibPGlite


def _native_tests_enabled() -> bool:
    return os.environ.get("PGLITE_RUN_NATIVE_TESTS") == "1"


def _native_library_built() -> bool:
    try:
        _LibPGlite._find_library()
    except Exception:
        return False
    return True


@unittest.skipUnless(_native_tests_enabled(), "set PGLITE_RUN_NATIVE_TESTS=1 to exercise libpglite")
@unittest.skipUnless(_native_library_built(), "build libpglite before running native integration tests")
class NativeIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory(prefix="pglite-native-test-")
        self.base_dir = Path(self._tempdir.name)

    def tearDown(self) -> None:
        self._tempdir.cleanup()

    def _run_native_child_capture(
        self,
        script: str,
        data_dir: Path,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = str(ROOT) if not existing else f"{ROOT}{os.pathsep}{existing}"
        if extra_env:
            env.update(extra_env)
        proc = subprocess.run(
            [sys.executable, "-c", script, str(data_dir)],
            capture_output=True,
            text=True,
            env=env,
        )
        if proc.returncode != 0:
            self.fail(
                "native child process failed\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        return proc

    def _run_native_child(
        self,
        script: str,
        data_dir: Path,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self._run_native_child_capture(script, data_dir, extra_env=extra_env)

    def test_native_bundle_keeps_template_fallback(self) -> None:
        lib_path = _LibPGlite._find_library()
        bundle_root = lib_path.parent.parent
        self.assertFalse((bundle_root / "bin" / "initdb").exists())
        self.assertTrue((bundle_root / "share" / "pglite-template").exists())

    @unittest.skipUnless(sys.platform == "darwin", "Mach-O install names are macOS-specific")
    def test_native_bundle_uses_relative_install_names(self) -> None:
        lib_path = _LibPGlite._find_library()
        libpq_path = lib_path.parent / "libpq.5.dylib"
        repo_libpq = ROOT / "postgres-pglite" / "pglite" / "out" / "native" / "lib" / "libpq.5.dylib"
        libpglite_output = subprocess.check_output(["otool", "-L", str(lib_path)], text=True)
        libpq_output = subprocess.check_output(["otool", "-D", str(libpq_path)], text=True)

        self.assertIn("@loader_path/libpq.5.dylib", libpglite_output)
        self.assertNotIn(str(repo_libpq), libpglite_output)
        self.assertIn("@loader_path/libpq.5.dylib", libpq_output)

    def test_native_bundle_exposes_linker_friendly_libpglite_names(self) -> None:
        lib_path = _LibPGlite._find_library()
        lib_dir = lib_path.parent

        if sys.platform == "darwin":
            self.assertTrue((lib_dir / "libpglite.dylib").exists())
        else:
            self.assertTrue((lib_dir / "libpglite.so.0.1").exists())
            self.assertTrue((lib_dir / "libpglite.so.0").exists())
            self.assertTrue((lib_dir / "libpglite.so").exists())

    def test_first_open_is_quiet(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db:
                result = db.query("select 1 as value")
                assert result.rows == [("1",)], result.rows
            """
        )
        proc = self._run_native_child_capture(script, self.base_dir / "quiet-open-pgdata")
        assert proc.stdout == "", proc.stdout
        assert proc.stderr == "", proc.stderr

    def test_direct_api_reports_backend_errors(self) -> None:
        script = textwrap.dedent(
            """
            import binascii
            import sys
            from pathlib import Path
            from pypglite import PGlite, PGliteBackendError

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db:
                raw = db.exec_raw("selekt 1")
                encoded = binascii.hexlify(raw).decode("ascii")
                assert encoded.startswith("45"), encoded
                assert "5a0000000549" in encoded, encoded

                try:
                    db.query("selekt 1")
                except PGliteBackendError as exc:
                    assert exc.sqlstate == "42601", exc.sqlstate
                    assert exc.message_primary == 'syntax error at or near "selekt"', exc.message_primary
                else:
                    raise AssertionError("expected syntax error")
            """
        )
        self._run_native_child(script, self.base_dir / "direct-api-error-pgdata")

    def test_direct_api_smoke_query(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db:
                result = db.query("select 1 as value")
                assert result.rows == [("1",)], result.rows
                assert result.named_rows == [{"value": "1"}], result.named_rows
            """
        )
        self._run_native_child(script, self.base_dir / "direct-api-pgdata")

    def test_direct_api_template_bootstrap_mode(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir, bootstrap_mode="template") as db:
                result = db.query("select 1 as value")
                assert result.rows == [("1",)], result.rows
                assert result.named_rows == [{"value": "1"}], result.named_rows
            """
        )
        self._run_native_child(script, self.base_dir / "direct-api-template-pgdata")

    def test_direct_api_multiple_handles_share_one_embedded_engine(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db1:
                with PGlite(data_dir) as db2:
                    db1.query("create table demo (id int primary key, name text)")
                    db1.query("insert into demo values (1, 'alpha')")
                    result = db2.query("select id, name from demo order by id")
                    assert result.rows == [("1", "alpha")], result.rows
                    assert result.named_rows == [{"id": "1", "name": "alpha"}], result.named_rows

                follow_up = db1.query("select count(*) as total from demo")
                assert follow_up.rows == [("1",)], follow_up.rows
                assert follow_up.named_rows == [{"total": "1"}], follow_up.named_rows
            """
        )
        self._run_native_child(script, self.base_dir / "direct-api-shared-engine-pgdata")

    def test_direct_api_named_rows_preserve_duplicate_aliases(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db:
                result = db.query("select 0.0::float8 as coalesce, false::bool as coalesce")
                row = result.named_rows[0]
                assert row["coalesce"][1] == "f", row["coalesce"]
                assert float(row["coalesce"][0]) == 0.0, row["coalesce"]
                assert row.items()[0][0] == "coalesce", row.items()
                assert row.items()[1] == ("coalesce", "f"), row.items()
                assert row.as_dict()["coalesce"][1] == "f", row.as_dict()
                assert float(row.as_dict()["coalesce"][0]) == 0.0, row.as_dict()
            """
        )
        self._run_native_child(script, self.base_dir / "direct-api-duplicate-alias-pgdata")

    def test_vector_extension_is_available_in_native_bundle(self) -> None:
        script = textwrap.dedent(
            """
            import sys
            from pathlib import Path
            from pypglite import PGlite

            data_dir = Path(sys.argv[1])
            with PGlite(data_dir) as db:
                result = db.query("create extension if not exists vector")
                assert result.command_tag == "CREATE EXTENSION"
                result = db.query("select extname from pg_extension where extname = 'vector'")
                assert result.rows == [("vector",)]
                result = db.query("select vector_dims('[1,2,3]'::vector)")
                assert result.rows == [("3",)], result.rows
            """
        )
        proc = self._run_native_child_capture(script, self.base_dir / "vector-extension-pgdata")
        assert proc.stdout == "", proc.stdout
        assert proc.stderr == "", proc.stderr


if __name__ == "__main__":
    unittest.main()
