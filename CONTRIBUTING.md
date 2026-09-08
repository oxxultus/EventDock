# EventDock contribution conventions

## Architecture

- Dependencies must point inward: framework/technology adapters → feature core → `eventdock-core`.
- Core modules must not depend on Spring, Jakarta Persistence, Kafka, Jackson, Lombok, or a database driver.
- Outbox and inbox remain independently usable. They share only contracts from `eventdock-core`.
- A new database, broker, codec, or framework is added as a new adapter module; existing core changes only when domain policy or a public contract changes.
- Starter and auto-configuration modules only assemble objects, bind configuration, and manage lifecycle. Business rules belong in core modules.
- Public APIs use neutral identifiers such as `String`; technology-specific types stay inside adapters.

## Module naming

- Core contract: `eventdock-core`
- Feature: `eventdock-<feature>`
- Storage adapter: `eventdock-storage-<database>`
- Transport adapter: `eventdock-transport-<broker>`
- Codec adapter: `eventdock-codec-<format>`
- Framework integration: `eventdock-<framework>-autoconfigure` or `eventdock-<framework>-starter`

Java packages use `io.github.oxxultus.eventdock.<area>`.

## Code

- Java 21 is the baseline.
- Prefer immutable records and constructor injection.
- Validate public boundary inputs and provide stable, actionable error messages.
- Do not hide blocking I/O, start transactions, or create threads inside core logic.
- Use `Clock` instead of direct system-time calls in core logic.
- Preserve binary and source compatibility within a major version. Deprecate before removal where practical.

## Tests

- Unit-test core policies without a framework.
- Adapter integration tests use the real technology through Testcontainers where applicable.
- Concurrency tests must cover duplicate claims, expired-lock recovery, and retry exhaustion.
- Transaction tests must prove that business rollback also rolls back an outbox append.
- Every bug fix includes a regression test.

## Git workflow

- `main` is always releasable.
- Create branches from current `main`.
- Use `feature/<issue>-<short-name>`, `fix/<issue>-<short-name>`, `refactor/<issue>-<short-name>`, or `docs/<issue>-<short-name>`.
- Keep one objective per issue, branch, and pull request.
- Merge through a pull request after CI passes and at least one review.
- Prefer squash merge; delete the branch after merge.

Commit format:

```text
Type: concise English subject

- What changed
- Why it changed
```

Allowed types: `Feat`, `Fix`, `Refactor`, `Test`, `Docs`, `Build`, `CI`, `Chore`, `Perf`, `Revert`.

## Pull requests

Each pull request documents:

- change and motivation;
- affected public API and compatibility;
- tests performed;
- database, configuration, migration, and operational impact;
- linked issue using `Closes #<number>` when applicable.

## Releases

- Follow Semantic Versioning.
- Maintain one aligned version across all modules.
- Record user-visible changes in `CHANGELOG.md`.
- Breaking public API or database schema changes require a major release after `1.0.0`.
- Adapter additions are minor releases; compatible fixes are patch releases.
- Never publish snapshots as stable releases.
