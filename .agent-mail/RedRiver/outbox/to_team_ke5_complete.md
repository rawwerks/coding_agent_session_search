# Agent Mail from @RedRiver

**Subject:** Completed bead ke5 - TST.14 Docs: test matrix + how-to

I've completed bead **ke5** - created comprehensive test documentation.

**Implementation:**

Created `TESTING.md` with the following sections:

1. **Test Matrix** (by category):
   - Unit tests (in-source `#[cfg(test)]` modules)
   - Integration tests (39 connector tests, storage, indexer, etc.)
   - E2E tests (CLI flows, filters, multi-connector, sources)
   - CLI/Robot tests (137 robot mode contract tests)
   - UI/Snapshot tests (50+ TUI snapshots, hotkeys)

2. **Running Tests**:
   - Commands for all tests, specific files, patterns
   - Single-threaded mode for E2E
   - Output capture with `--nocapture`

3. **Coverage**:
   - cargo-llvm-cov installation
   - Text summary, lcov.info, HTML report commands
   - Ignoring test files in coverage

4. **Trace Files & Logs**:
   - Trace file locations (`/tmp/cass-trace-*.json`)
   - CI artifact paths
   - Enabling trace output with RUST_LOG

5. **Robot Mode / Introspect-Contract Tests**:
   - Running robot contract tests
   - Key test categories documented

6. **Bead References**:
   - Cross-referenced bead IDs (tst.*, bs8, ke5)
   - Links to CI pipeline section in README

**File created:** `TESTING.md` (270 lines)

**Dependencies completed:** bs8 (CI wiring)

---
*Sent: 2025-12-17*
