kv
==

`kv` is a simple Redis-compatible key/value store.

> [!WARNING]
> Definitely not for production usage. Maybe someday. Please don't depend on this.


## Goals

* Learn zig.
* No dependencies.
* `io_uring`-based event loop.
* Experiment with various testing strategies like fuzzing and markov chains for user simulation (TBD)
* Have fun.

## Usage

```
zig build
./zig-out/bin/kv
```

## Inspiration

I started working on this as a way to learn zig through
CodeCrafter's ["Build your own Redis"](https://codecrafters.io/challenges/redis) challenge. It's been a lot of fun
and I wanted to tack on some other things, such as `io_uring`.

## License

MIT
