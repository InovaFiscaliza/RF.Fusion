# AGENTS.md

## Mandatory context

Before answering, planning, editing, or refactoring code in this repository, read:

1. ARCHITECTURE.md
2. INSTRUCTIONS.md

These files are mandatory project context.

## Authority order

If there is any conflict, follow this order:

1. ARCHITECTURE.md
2. INSTRUCTIONS.md
3. Existing code

## Non-negotiable rules

- Do not change business logic unless explicitly requested.
- Do not invent architecture.
- Do not move functions to new modules unless allowed by ARCHITECTURE.md.
- Follow the worker anatomy and canonical loop from ARCHITECTURE.md.
- Follow the refactoring rules from INSTRUCTIONS.md.
- Use `config.py` constants instead of magic literals.
- Use `err.capture(...)`, never `err.set(...)`.
- Helpers raise exceptions; they do not return sentinel values for failure.
- Type DB handlers with concrete classes.
- Preserve existing database behavior and task lifecycle.

## Required response format for code changes

Before changing code, explain:

1. Which rule from ARCHITECTURE.md or INSTRUCTIONS.md applies.
2. Which files will be touched.
3. Whether the change is architectural, refactoring-only, or behavior-changing.

After changing code, report:

1. What changed.
2. What was not changed.
3. Validation commands or tests to run.
4. Any risk or manual review point.