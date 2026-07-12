# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` is the binary entry point; `src/fs.rs` contains filesystem-facing logic.
- `src/backend/` hosts storage backends and cache/dedup logic (`cache.rs`, `dedup.rs`, `general.rs`).
- `src/image/` handles image formats and raw parsing (`mod.rs`, `raw.rs`).
- `src/utils/` contains shared helpers and test utilities.
- Unit tests live beside the code they cover. Integration, CLI smoke, Redis,
  and privileged mount tests live under `tests/`.

## Build, Test, and Development Commands
- `cargo build --locked` builds the binary in debug mode.
- `cargo build --locked --release` produces an optimized release build.
- `cargo run -- --help` runs the CLI and prints usage/options.
- `cargo test --locked` runs unit and non-privileged integration tests.
- `cargo fmt --all -- --check` checks formatting.

## Coding Style & Naming Conventions
- Rust 2021 edition; follow standard rustfmt defaults (no repo-specific config).
- Use `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Prefer function and variable names that are both clear and as short as practical.
- Keep modules cohesive: backend logic stays under `src/backend/`, image parsing under `src/image/`.

## Testing Guidelines
- Prefer small, deterministic unit tests; use `tempfile` for filesystem-related cases.
- Run `cargo test --locked` before submitting changes that affect core logic.
- Redis integration tests are gated by the Cargo feature
  `redis-integration-tests`; they are not compiled in the default
  `cargo test` path.
- Run Redis integration tests with:
  `cargo test --locked --features redis-integration-tests redis_ -- --test-threads=1`.
- Redis integration tests require
  `DISTILL_FS_TEST_REDIS_URL=redis://host:6379/<non-zero-db>`.
- Because Redis integration tests `FLUSHDB` the target database before
  and after each test, the test harness rejects database zero and URLs without
  an explicit database number.
- Privileged FUSE tests are ignored by default. See `README.md` for their
  prerequisites and explicit commands.
- Nydus privileged tests build RAFS v5 images; RAFS v6 is intentionally
  rejected until the read path is moved to Nydus' `BlobDevice` flow.

## Commit & Pull Request Guidelines
- Commit messages in history are short, imperative, and capitalized (e.g., “Add support for …”).
- Keep the subject line focused on the change’s outcome; use the body for rationale when needed.
- Subject line (first line): maximum 72 characters.
- Body lines: maximum 72 characters per line.
- Remove unnecessary blank lines in commit messages; keep body compact.
- Before each commit, run `cargo fmt` to ensure formatting is correct.
- Use conventional commit prefixes: `feat:`, `fix:`, `refactor:`, `perf:`, `docs:`, `test:`, `chore:`.
- When creating or amending commits from the agent, write the message to a file and use `git commit -F <file>` instead of inline `-m` text.
- PRs should include: a clear description, testing notes (`cargo test`, `cargo build`), and any relevant logs or screenshots if behavior changes.

## Agent-Specific Notes
- Keep changes minimal and localized; avoid reformatting unrelated code.
- If you add new modules, update `mod.rs` accordingly and keep naming consistent with existing layout.
- For build/test/validation in this repo, prefer the `pod-build-test` skill and run inside the configured Kubernetes pod instead of local macOS. Use pod execution for the repo lint step as well, preferably via the `pod-build-test` skill's `lint` action.
- When running Redis integration tests in the pod, prefer:
  `pod-run test --repo-root <repo> --cmd 'cargo test --features redis-integration-tests redis_ -- --test-threads=1'`.
  The `pod-run` entrypoint comes from the `pod-build-test` skill, not
  from this repository.

## Skills
- When resolving a skill, check the current project first, then the global Codex configuration directory.
- Project-local skills live under `.codex/skills/` in this repository and take precedence when a skill with the same name exists in multiple locations.
- If the skill is not present in the project, fall back to `$CODEX_HOME/skills/` (for example, `~/.codex/skills/`).
