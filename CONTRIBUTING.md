# Contributing to shunt

Thanks for your interest. shunt is a small, dependency-free Go service that you
can read end to end in an afternoon — contributions are very welcome.

## Project layout

| Path | Purpose |
|------|---------|
| `cmd/shunt`        | entrypoint + configuration |
| `internal/forge`   | Forgejo/Gitea REST API client |
| `internal/gitops`  | staging-branch git plumbing (shells out to `git`) |
| `internal/engine`  | the reconcile loop: rollup batching + bisection |
| `internal/manager` | multi-repo discovery + one engine per repo |

The algorithm and the (sometimes surprising) Forgejo mechanics it relies on are
documented in [`docs/design.md`](docs/design.md).

## Develop

```sh
make build   # static binary
make test    # unit tests — the bisection state machine is mock-driven
make lint    # gofmt + go vet (the CI gate)
```

The Makefile defaults to the patched Go 1.25.12 toolchain. Override
`GOTOOLCHAIN` only with another release that includes the same security fixes.
There are **no external dependencies** — please keep it that way unless there's
a compelling reason.

## Pull requests

- Keep changes small and focused.
- Add or adjust tests for any behaviour change (the engine is interface-driven
  precisely so it can be tested without a live forge).
- `make lint test` must pass; keep `gofmt` clean.
- Clear, present-tense commit subjects are appreciated.
- Do not include private instance URLs, local filesystem paths, tokens, account
  IDs, or environment-specific operational details in code, fixtures, examples,
  docs, or comments.

## Agent-assisted work

Agent instructions live in [`AGENTS.md`](AGENTS.md). They are part of the
contribution policy: use a separate worktree, keep secrets out of prompts and
diffs, run the project validation commands, and use an independent review pass
for non-trivial code changes.

## Testing against a real instance

`docs/design.md` describes the validated Forgejo mechanics and a safe way to
exercise shunt end to end against a throwaway repo on a test instance. The
env-gated forge client harness is documented in
[`docs/forge-integration-tests.md`](docs/forge-integration-tests.md).
