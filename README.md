kv
==

`kv` is a simple Redis-compatible key/value store.

> [!WARNING]
> There are tons of sharp edges in this codebase and obvious cases where errors are not handled correctly.
> Please do not depend on it in production!


## Goals

* Learn Zig.
* No dependencies.
* Static memory allocation.
* `io_uring` integration for connection handling.
* Experiment with various testing strategies like fuzzing
* Have fun.

## Non-goals

* Full compatibility with Redis protocol.
* Support for non-Linux systems.

## Usage

```
zig build
./zig-out/bin/kv
```

## License

MIT
