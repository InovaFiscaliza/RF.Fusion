## Comments and maintainability

Write comments for:
- intent;
- architectural decisions;
- operational context;
- non-obvious behavior;
- business rules;
- edge cases;
- limitations and assumptions.

Do NOT write comments that merely describe obvious syntax.

Bad:
    # Increment counter
    counter += 1

Good:
    # Counter is intentionally monotonic because workers may retry
    # the same task concurrently after process crashes.
    counter += 1

Prefer comments explaining:
- WHY something exists;
- WHY a specific approach was chosen;
- WHAT can break if modified incorrectly;
- external system constraints;
- performance considerations;
- concurrency assumptions;
- integration quirks.

Add short docstrings to important functions containing:
- purpose;
- inputs;
- outputs;
- side effects;
- expected exceptions when relevant.

For long or critical flows:
- add section comments separating logical blocks;
- explain the lifecycle/state transitions;
- explain interactions with database/external systems.

When modifying existing code:
- preserve useful comments;
- improve outdated comments;
- do not remove operational context comments.

Avoid:
- redundant comments;
- decorative comments;
- excessive banner comments;
- commenting every line.

The code should be understandable by an engineer debugging production issues at 2 AM.