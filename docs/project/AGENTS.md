# EventDock agent instructions

[English](AGENTS.md) | [한국어](AGENTS.ko.md)

- Follow the root `../../CONTRIBUTING.md` for every change.
- Preserve the inward dependency rule and keep core modules framework-independent.
- Do not add a dependency to a core module without checking that it is technology-neutral and essential.
- Add or update focused tests with behavior changes.
- Do not commit directly to `main` after the initial repository bootstrap; use an issue branch and pull request.
- Before release-related work, verify tests, public API compatibility, migration impact, and `CHANGELOG.md`.
