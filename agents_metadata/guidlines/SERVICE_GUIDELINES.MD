# Service Guidelines

- Put business logic, orchestration, validation, and domain state transitions in services, not in thin entrypoints such as CLI handlers, notebooks, scripts, or temporary glue code.
- Use services for bounded domain operations and reusable workflow logic.
- Use job or experiment modules for scheduled execution flows such as Flink streaming jobs, Spark recalibration runs, simulations, benchmarks, and reproducible experiments.
- Use adapters for external systems and infrastructure boundaries such as Kafka, Redis, PostgreSQL, Flink connectors, Spark sessions, filesystem access, and HTTP clients.
- Keep entrypoints thin: parse inputs, construct dependencies, invoke the owning service or job, and shape outputs.
- Job entrypoints should load the YAML config, merge explicit CLI overrides, validate the final Pydantic config, construct dependencies from that config, and then invoke the owning service.
- Validation and data formatting helpers may prepare inputs and outputs, but they should not orchestrate domain workflows that belong in services or jobs.
- When one service needs another, make the dependency explicit through constructor injection, owned fields, or a clear module boundary.
- Service methods should be typed. New non-trivial methods should have clear docstrings describing purpose, inputs, outputs, and raised domain exceptions.
- Keep service methods explicit about the boundary they own. If logic belongs to a more specific component, move it there instead of duplicating wrappers in a broader module.
- Raise domain-specific custom exceptions for expected failures. Preserve more specific downstream exceptions instead of flattening them into generic framework errors.
- Keep logs in services, jobs, and integration adapters for business failures, state transitions, integrations, and recoverable/internal error paths.
- Every job must emit exactly one final structured summary. The success summary should be visually clear and logged or printed through the project output helpers; the failure summary should be visually clear and use the project failure output path. Include the job name, status, relevant configuration, core counts, duration when available, and failure reason when applicable.
- Avoid hidden side effects in unrelated layers. If a module owns a workflow, it should be the clear entry point for that workflow.
- Reuse existing service methods when an appropriate one already exists. If the correct domain method does not exist, add it in the appropriate owning module instead of bypassing the framework structure.
- Design services so they can be composed for experiments, benchmarks, simulations, and alternate CTR modeling strategies without rewriting the surrounding flow.
