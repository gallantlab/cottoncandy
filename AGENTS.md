# AGENTS.md

Guidance for AI coding agents working in this repository.

cottoncandy ("sugar for S3") is a pure-Python library for storing and loading numpy arrays (and JSON, pickles, files, dask/sparse arrays) in S3-compatible object stores, Google Drive, or a local directory. Arrays are streamed to and from memory, never staged on disk. Built on boto3.

## Setup

```bash
uv sync --all-extras        # creates .venv with the dev group (tests + docs) and scipy/dask
uv run python -c "import cottoncandy"
```

- Version is a hand-edited string, `__version__` in `cottoncandy/__init__.py` (read dynamically by `pyproject.toml`).
- `boto3<1.36` is pinned deliberately: newer boto defaults break several third-party S3 services. Leave the pin when touching dependencies.
- CI covers Python 3.10 to 3.14; keep code compatible with 3.10.
- All new code must have type annotations (parameters and return types), using `X | Y` and `X | None` rather than `Union`/`Optional`. `localclient.py` is the style reference.
- A new git worktree gets its own `.venv`; rerun `uv sync` there.

## Tests

```bash
uv run pytest                                   # full suite, coverage on (addopts in pyproject.toml)
uv run pytest cottoncandy/tests/test_localclient.py -k name --no-cov -x
```

- The `cci` fixture in `cottoncandy/tests/conftest.py` is parametrized over backends. `local` always runs (temp dir). `s3` is added only when `DL_BUCKET_NAME`, `DL_ACCESS_KEY`, `DL_SECRET_KEY` and `DL_URL` are all set, which is the case in upstream CI (Wasabi) and normally not on a dev machine. Expect `local`-only runs locally.
- Tests that should hold for every backend belong in `test_roundtrip.py` using the `cci` fixture. Local-only behaviour goes in `test_localclient.py`.
- Tests write under a unique `testcc_<id>` prefix, which the fixture removes at the end of the session.
- `test_roundtrip_big.py` moves 200 MB arrays. Expect it to be slow.
- CI runs `pytest --reruns 2` because S3 listing lags. Gdrive has no automated tests.
- Lint: CI fails only on flake8 `E9,F63,F7,F82`; the rest of flake8 is advisory (max line length 127). Codespell runs on every PR.

## Architecture

Layered, with one abstract backend underneath a mixin-composed interface:

```
get_interface() -> DefaultInterface
                     = FileSystemInterface + ArrayInterface + BasicInterface   (interfaces.py)
                       |
                       self.backend_interface : CCBackEnd                       (backend.py)
                         S3Client (s3client.py) | GDriveClient (gdriveclient.py) | LocalClient (localclient.py)
```

- **`interfaces.py`**: the public API (`upload_raw_array`, `download_json`, `ls`, `glob`, `cp`, `mv`, `rm`, ...). `BasicInterface` handles objects, JSON, pickle and files; `ArrayInterface` adds numpy/dask/sparse arrays; `FileSystemInterface` adds path-like operations. Backend selection happens in `BasicInterface.__init__` (`backend='s3'|'gdrive'|'local'`).
- **`backend.py`**: `CCBackEnd` is the contract every client implements, and `CloudStream` wraps a stream plus its metadata. A new backend subclasses `CCBackEnd` and is wired into `BasicInterface.__init__` and `get_interface`.
- **Backend asymmetries**: S3 stores metadata as native object metadata. `LocalClient` writes a `<name>.meta.json` sidecar (`METADATA_SUFFIX`), so any listing/copy/move/delete code on the local backend must keep the sidecar in step with its file. GDrive uses file properties and has its own `glob_google_drive` path. Interface-level changes need a check against all three.
- **Arrays**: `upload_raw_array` stores raw bytes with dtype/shape/order in metadata (optionally compressed through `numcodecs`; `gzip` below 2 GB, `Zstd` above, per `[compression]` in the config). `upload_npy_array` uses the `.npy` format. Large uploads switch to multipart above `mpu_use_threshold`.
- **Groups and datasets**: `dict2cloud`/`cloud2dict` store a dict of arrays as a directory-like "group"; extensions (`.grp`, `.arr`, ...) come from the `[extensions]` config section.
- **`browser.py`**: tab-completion browser (`get_browser`). Path characters are mangled into attribute names (`.` becomes `_DOT_`).
- **`utils.py`**: path helpers (`SEPARATOR`, `remove_root`), metadata sanitizing, ndarray chunking for multipart/dask uploads, credential lookup (`get_keys`).

## Configuration

- `cottoncandy/defaults.cfg` holds the defaults. `options.py` loads it, then overlays the per-user `options.cfg` (platform user-data dir, via `appdirs.py`), writing that file on first import.
- Module-level constants in `__init__.py` (`ACCESS_KEY`, `default_bucket`, ...) and the defaults in `interfaces.py` are read once at import time. Changing the config mid-session needs a reload, and tests should pass explicit arguments to `get_interface` instead.
- `appdirs.py` is vendored; leave it as is (excluded from ruff).
- A new config key needs an entry in `defaults.cfg`. Existing users' `options.cfg` files do not get new keys automatically, so add a fallback or a migration step in `options.py` (see the `gdrive` section handling there).

## Docs

```bash
cd gendoc && uv run make githubio-docs     # output lands in ../docs (gitignored)
```

- `uv run` is required: the Makefile calls bare `python3` and `sphinx-build`, which only the project venv provides.
- The build regenerates the tracked pages in `gendoc/source/api/` (and can delete or rewrite them). Run `git checkout -- gendoc/source/api` afterwards unless the API change is intentional.
- Those pages come from docstrings (numpydoc style), so write public docstrings in that format. The docs workflow publishes to `gh-pages` on tags or a manual dispatch on `main`.

## Gotchas

- `import cottoncandy` creates `options.cfg` in the user's config dir on first run, so the directory must be writable (point `XDG_CONFIG_HOME`/`HOME` at a temp dir in restricted environments).
- Never commit real credentials. Tests and examples take keys from environment variables or the config file.
- The README and the notebooks in `cottoncandy/examples/` document the public API. Update them when a public signature changes.
