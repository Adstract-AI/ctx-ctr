# Error Handling Guidelines

- Use custom project exceptions for expected business or framework failures; do not raise generic `Exception` for domain errors.
- Custom exceptions should carry stable, explicit meaning for the caller, such as a clear error type, code, or structured context appropriate to the interface.
- Preserve the most specific exception available; do not wrap validation, parsing, configuration, storage, integration, or domain exceptions into broader generic errors unless translation is required at a boundary.
- Raise business and domain exceptions from services, adapters, and job orchestration modules; keep entrypoints thin and focused on input parsing, invocation, and output shaping.
- Keep logs in services, adapters, jobs, and integration layers.
- Separate expected operational failures from true internal failures. Model these paths explicitly instead of collapsing them into a single generic error outcome.
- If a failure is internal but state, artifacts, or error tracking must still be persisted first, persist them before raising the internal failure.
- If an operation has recoverable or non-fatal errors versus fatal errors, model those paths explicitly; do not report a normal success outcome for fatal internal failures.
- Do not swallow exceptions silently. Either handle them intentionally with logging, persistence, fallback behavior, or re-raise the appropriate custom exception.
- When catching broad exceptions to translate them, preserve domain-specific exceptions with explicit `except SomeDomainException: raise` branches before the generic catch.
- Error messages and surfaced error payloads should not expose internal implementation details unless explicitly required. Prefer safe, domain-level messages.
- Keep exception names and error messages specific to the bounded context and operation that failed.
