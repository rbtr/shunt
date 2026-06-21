# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-06-21

Initial release.

### Added
- Rollup-batch + **bisection** merge queue for Forgejo (Gitea-compatible API).
- Auto-merge-button trigger — PRs enter the queue via "Merge when checks
  succeed", detected from the PR timeline. No bot commands.
- Status-gated native merge landing (sets a required `merge-queue` commit status
  then merges via the API); bisection isolates the bad PR(s) on gate failure.
- Multi-repo discovery by topic, with automatic branch-protection setup.
- Single static binary + container image; `/healthz` endpoint.
- Mock-driven unit tests for the bisection state machine.

### Security
- Git remotes are kept credential-free; staging pushes authenticate through
  non-interactive Git credential prompts instead of embedding tokens in clone
  URLs.

[Unreleased]: https://github.com/rbtr/shunt/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rbtr/shunt/releases/tag/v0.1.0
