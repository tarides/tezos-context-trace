# Tezos Context Trace

Tools for working with Tezos Context traces.

## Overview

`tezos-context` is a part of the [Tezos Octez implementation](https://gitlab.com/tezos/tezos) that uses [Irmin](https://github.com/mirage/irmin). The Irmin team has created a shim around `tezos-context` that records all `tezos-context` API calls and records them in a trace. This trace (after some pre-processing) is used to replay `tezos-context` API calls with different versions of Irmin in order to benchmark performance of changes to Irmin.

The code in this repository is organized into two subpackages of `tezos-context`:

- [`tezos-context.trace`](./src/trace/): Holds type definitions for trace events and implements a serialization based on [repr](https://github.com/mirage/repr).
- [`tezos-context.disk-recorder`](./src/disk-recorder/): An implementation of `Tezos_context_disk` that that records API calls into a trace.

An additional package provides tooling:

- [`tezos-context-trace-tools`](./src/tools/): Tools for working with traces. This includes the `tezos-context-replay` tool that is used for benchmarking.

## Recording a new trace

In order to record a new trace of `tezos-context` API calls the subpackages `tezos-context.trace` and `tezos-context.disk-recorder` need to be vendored into a checkout of Tezos:


```
ln -s ../tezos-context-trace/src/trace vendors
ln -s ../tezos-context-trace/src/disk-recorder vendors
```

It is important to link in only the subdirectories. If you link in the entire repo, dune will complain about multiple `tezos-context.opam` files.

You will also have to vendor the `printbox` and `bentov` libraries:

```
ln -s ../printbox vendors
ln -s ../bentov vendors
```

Finally, you will have to patch Tezos to use `tezos-context.disk-recorder` instead of `tezos-context.disk`. See the [irmin-tezos-benchmarking](https://github.com/tarides/irmin-tezos-benchmarking) repo for more information.
