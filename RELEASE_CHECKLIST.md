# Release Checklist (PyPI)

Use this checklist before publishing a new version of `climasus4py`.

## 1. Versioning and metadata

- [ ] Update `version` in `pyproject.toml` (semantic versioning).
- [ ] Confirm package metadata: `name`, `description`, `readme`, `license`, `requires-python`.
- [ ] Ensure `LICENSE` file exists and matches metadata.

## 2. Quality gates (must pass)

- [ ] Lint: `ruff check .`
- [ ] Tests: `pytest -q`
- [ ] Docs build: `mkdocs build --strict`
- [ ] Package build: `python -m build`
- [ ] Distribution checks: `twine check dist/*`

## 3. Documentation and changelog

- [ ] Update README if public behavior changed.
- [ ] Update trilingual docs (`docs/pt`, `docs/en`, `docs/es`) for API changes.
- [ ] Add release notes / changelog entry.

## 4. TestPyPI validation (recommended)

- [ ] Publish to TestPyPI via workflow dispatch (`target=testpypi`).
- [ ] Install from TestPyPI in a clean environment and run smoke tests.

## 5. Production release (PyPI)

- [ ] Create and push tag: `vX.Y.Z`.
- [ ] Confirm publish workflow succeeded.
- [ ] Verify package page on PyPI.

## 6. Post-release checks

- [ ] Install from PyPI: `pip install climasus4py==X.Y.Z`.
- [ ] Run a quick pipeline smoke test.
- [ ] Announce release with highlights and migration notes (if any).

## Trusted Publishing setup (GitHub -> PyPI)

Configure PyPI and TestPyPI to trust this repository's workflow for OIDC publishing.

- PyPI project settings -> Publishing -> Add trusted publisher
- Repository: `climasus/climasus4py`
- Workflow: `.github/workflows/publish-pypi.yml`
- Environment (recommended): `pypi` and `testpypi`

This avoids storing long-lived API tokens in GitHub secrets.
