# No-Mock Audit Report

Generated: 2026-01-27 (post vhl0 real-model refactor, updated post e2e_ssh_sources fix)

## Executive Summary

This audit catalogs remaining mock/fake/stub patterns in the cass codebase.

**Status:** ✅ All violations resolved.

**Current allowlist:** 2 entries (deterministic fixture constructors)
- `mock_system_info`
- `mock_resources`

**Matches found:** 29 total
- 29 matches in `src/sources/install.rs` (fixture constructors, allowlisted)

**CI validation:** `./scripts/validate_ci.sh --no-mock-only` should pass.

## Classification Categories

- **(a) REMOVE/REPLACE**: Mock that should be replaced with real implementation
- **(b) CONVERT TO FIXTURE**: Mock data that should use real recorded sessions/data
- **(c) ALLOWLIST**: True platform boundary or deterministic fixture constructor

---

## Source Code (`src/`)

### 1. `src/sources/install.rs`

**Classification: (c) ALLOWLIST - Deterministic fixture constructors**

Patterns:
- `mock_system_info`
- `mock_resources`

**Decision:** These helpers construct `SystemInfo` / `ResourceInfo` for pure
function unit tests (install method selection and resource checks). They are
non-network, deterministic fixtures and are complemented by real system probe
integration tests.

**Review date:** 2026-07-27

---

## Test Files (`tests/`)

No violations remain. The `tests/e2e_ssh_sources.rs` file previously used
`fake_bin` / `fake_rsync` naming, which has been fixed to `fixture_bin` /
`fixture_rsync`.

---

## Change Log

- 2026-01-27: Fixed `tests/e2e_ssh_sources.rs` naming from `fake_*` to `fixture_*`.
- 2026-01-27: Removed MockEmbedder/MockReranker/MockDaemon tests in favor of
  real FastEmbed model fixtures (see vhl0). Allowlist reduced to install
  fixture constructors only.
