# Agent Guidance for HTFlow

Notes for AI coding agents working in this repo, based on how work here has actually gone.

## Workflow: investigate, propose, confirm, then implement

For anything beyond a trivial fix, don't jump straight to editing files:

1. Investigate the current code and state your findings plainly (file:line references).
2. Propose the concrete change — show the actual diff/design, not just a description — before applying it.
3. Wait for explicit confirmation. "Let's chat about solutions" means discuss tradeoffs and options first, not start coding.
4. Only then implement.

When a design has more than one reasonable shape (e.g. a new config knob vs. reusing an existing one), lay out the options with their tradeoffs and a recommendation, and let the decision be made explicitly rather than picking silently.

## Scope discipline

Implement exactly what's asked — no extra flags, no speculative abstractions, no "while I'm here" cleanup. If a spec includes a worked example (e.g. specific input files mapping to specific output paths), trace your implementation against that example by hand before writing code, and match it exactly.

## Tests

- Every behavior change needs test coverage in the same change, including new/updated cases and any test-isolation fallout (e.g. a change that starts writing to a relative path needs cwd-isolated tests, or it'll pollute the real test-runner's directory).
- Check `tests/CMakeLists.txt` when adding a new test file — a test file with no `create_test(...)` entry never runs under `ctest`.
- Prefer asking before running `pytest`/`ctest` directly — often the expectation is to run it yourself and share the output rather than have the agent run it.
- `htcondor2` isn't always installed (no arm64 macOS wheel/build). Don't probe it directly (no `python3 -c "import htcondor2"`-style exploration) — trust what you're told about its behavior, and rely on `tests/conftest.py`'s mock fallback, which activates automatically when the real package isn't importable.

## Docs stay in sync

Every module under `htflow/` has a corresponding doc under `docs/`. When behavior changes, update the doc in the same pass — don't treat it as a follow-up. That includes `README.md` and `CONTRIBUTING.md` when the change is user- or contributor-facing (new flags, new setup steps, new CLI behavior).

## Git

Only commit or push when explicitly told to. Match the existing commit style: short present-tense summary line, body explaining *why* the change was made when it isn't obvious from the diff alone.
