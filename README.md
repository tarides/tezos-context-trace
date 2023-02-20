# Tezos Context Trace

`tezos_context` is a part of the Tezos Octez implementation that uses Irmin. The Irmin team has created a shim around `tezos_context` that records all `tezos_context` API calls and records them in a trace. This trace (after some pre-processing) is used to replay `tezos_context` API calls with different versions of Irmin in order to benchmark performance of changes to Irmin.

The code in this repository is organized into three libraries:

- `tezos-context-trace`: Holds type definitions for trace events and implements a serialization.
- `tezos-context-trace-recorder`: Implements a shim of the `tezos_context` library that records API calls into a trace.
- `tezos-context-trace-tools`: Tools for working with traces. This includes the `tezos-context-replay` tool that is used for benchmarking.
