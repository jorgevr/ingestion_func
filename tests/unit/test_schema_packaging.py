"""Every schema path the code loads at runtime must live inside a directory
the Dockerfile actually copies into the image.

Regression test for the historical_worker outage: `function_app.py` used to
build its work-item schema path from `specs/002-pvdaq-historical-ingestion/
contracts/...`, which is never copied into the image (only `function_app.py`,
`host.json`, `requirements.txt`, `src/`, and `schemas/` are). That path
resolved fine on every developer's laptop (the whole repo is on disk) and
raised `FileNotFoundError` on every single message in the container.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "Dockerfile"
_SCHEMAS_DIR = (_REPO_ROOT / "schemas").resolve()


def _dockerfile_copied_sources() -> set[str]:
    """Parse `COPY --chown=... <src...> <dest>` lines (excluding the
    build-stage `COPY --from=build ...` layer) and return the source paths
    actually copied into the runtime image."""
    text = _DOCKERFILE.read_text(encoding="utf-8")
    copied: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("COPY") or "--from=" in stripped:
            continue
        tokens = stripped.split()
        # First token is "COPY"; last is the destination; anything in
        # between that isn't a --flag is a source.
        sources = [t for t in tokens[1:-1] if not t.startswith("--")]
        copied.update(sources)
    return copied


def _schema_paths_loaded_by_code() -> dict[str, Path]:
    """Every module-level schema path constant the runtime code resolves."""
    import function_app
    from src import cloudevents_envelope, schema_validator

    return {
        "src/schema_validator.py:_SCHEMA_PATH": schema_validator._SCHEMA_PATH,
        "function_app.py:_WORK_ITEM_SCHEMA_PATH": function_app._WORK_ITEM_SCHEMA_PATH,
        "function_app.py:_METADATA_FILE_SCHEMA_PATH": (
            function_app._METADATA_FILE_SCHEMA_PATH
        ),
        "src/cloudevents_envelope.py:_DATASET_AVAILABLE_SCHEMA_PATH": (
            cloudevents_envelope._DATASET_AVAILABLE_SCHEMA_PATH
        ),
    }


class TestDockerfileCopiesSchemasNotSpecsOrDocs:
    def test_dockerfile_copies_the_schemas_directory(self) -> None:
        copied = _dockerfile_copied_sources()
        assert "schemas/" in copied, f"Dockerfile COPY sources: {copied}"

    def test_dockerfile_never_copies_specs_or_docs(self) -> None:
        copied = _dockerfile_copied_sources()
        assert not any(src.startswith("specs") for src in copied), copied
        assert not any(src.startswith("docs") for src in copied), copied


class TestEverySchemaPathIsInsideTheImage:
    def test_every_loaded_schema_path_exists_on_disk(self) -> None:
        for label, path in _schema_paths_loaded_by_code().items():
            assert path.exists(), f"{label} points at a missing file: {path}"

    def test_every_loaded_schema_path_is_under_schemas_directory(self) -> None:
        for label, path in _schema_paths_loaded_by_code().items():
            resolved = path.resolve()
            assert resolved.is_relative_to(_SCHEMAS_DIR), (
                f"{label} resolves to {resolved}, which is not inside "
                f"{_SCHEMAS_DIR} — the Dockerfile does not copy anything "
                "outside it, so this path would 404 in the container"
            )

    def test_no_loaded_schema_path_is_under_specs_or_docs(self) -> None:
        specs_dir = (_REPO_ROOT / "specs").resolve()
        docs_dir = (_REPO_ROOT / "docs").resolve()
        for label, path in _schema_paths_loaded_by_code().items():
            resolved = path.resolve()
            assert not str(resolved).startswith(str(specs_dir)), (
                f"{label} resolves under specs/: {resolved}"
            )
            assert not str(resolved).startswith(str(docs_dir)), (
                f"{label} resolves under docs/: {resolved}"
            )
