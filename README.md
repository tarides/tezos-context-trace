# Irmin/Tezos Benchmarking

This repository contains code, infrastructure descriptoin and instructions on how to run Irmin/Tezos Context benchmarks.

## Context

`tezos_context` is a part of the Tezos Octez implementation that uses Irmin. The Irmin team has created a shim around `tezos_context` that records all lib_context API calls and records them in a trace. This trace (after some pre-processing) is used to replay `tezos_context` API calls with different versions of Irmin in order to benchmark performance of changes to Irmin.

These benchmarks are central to the contractual obligations between Tarides and Tezos and usually need to be produced as part of a milestone.
