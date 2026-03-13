# project-alpha

An experimental ground-up reimplementation of `psql` in Rust.

## Why?

`psql` is written in C and tightly coupled to the PostgreSQL source tree. It can't be built or distributed independently. We wanted to see if a standalone Rust implementation could:

- Produce a single static binary with no dependencies
- Support cross-compilation to Linux, macOS, and Windows (x86_64 + aarch64)
- Use async I/O (`tokio-postgres`) instead of libpq
- Serve as a foundation for better tooling around the Postgres wire protocol

## Status

Early development. Working on core connectivity and meta-commands (`\d` family).

## Building

```bash
cargo build --release
```

## License

Apache 2.0
