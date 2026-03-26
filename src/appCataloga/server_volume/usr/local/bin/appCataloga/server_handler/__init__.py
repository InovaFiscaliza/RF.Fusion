"""
Runtime infrastructure shared by appCataloga entrypoints.

This package collects the mechanics that make daemons run, but are not
themselves part of backup/discovery business rules. In practice this means:

- socket and request-transport helpers
- signal and shutdown wiring
- detached worker-pool management
- small runtime primitives such as timeout and sleep helpers

The goal is to keep each `appCataloga_xxx.py` script focused on its own
service flow while still leaving the entrypoint as the visible owner of the
microservice lifecycle.
"""
