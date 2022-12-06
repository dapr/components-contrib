## Basic WebAssembly Middleware End-to-End Tests

This is an internal project holding source for various test guest wasm used to
cover corner cases. This has its own [go.mod](go.mod) and [Makefile](Makefile)
as the guest source is compiled with TinyGo, not Go. The `%.wasm` binaries here
are checked in, to ensure end-to-end tests run without any prerequisite setup.
