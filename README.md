# Tezos Context Trace

`tezos_context` is a part of the Tezos Octez implementation that uses Irmin. The Irmin team has created a shim around `tezos_context` that records all `tezos_context` API calls and records them in a trace. This trace (after some pre-processing) is used to replay `tezos_context` API calls with different versions of Irmin in order to benchmark performance of changes to Irmin.

This repository contains:

- The `tezos_context` shim that records a trace.
- Tools for post-processing and replaying traces.
