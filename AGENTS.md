# Repository guidance

This is a short map for working in dapr/components-contrib. Check the existing component and its tests before changing shared behavior.

## Repository layout

- Top-level building blocks include binarystore/, bindings/, configuration/, contenttype/, conversation/, crypto/, health/, lock/, middleware/, nameresolution/, pubsub/, search/, secretstores/, state/, and vector/.
- Components usually live under a building-block/provider directory. Component metadata is in that component's metadata.yaml. Some families use another level of nesting, such as middleware/http/.
- common/ contains reusable packages shared across components. metadata/ contains metadata support code.
- docs/ holds contributor and component documentation. tests/ holds shared test infrastructure and component test definitions.

## Build and checks

Run Make targets from the repository root. Use the Go version in go.mod and the golangci-lint version pinned by [.github/workflows/components-contrib.yml](.github/workflows/components-contrib.yml), currently v2.10.1.

- `make test` runs Go unit tests in the root module with the metadata build tag. It does not enter nested Go modules, including `tests/certification/` and `tests/e2e/pubsub/jetstream/`; several WebAssembly examples and fixtures elsewhere also have their own `go.mod` files.
- `make lint` runs golangci-lint across the repository.
- After Go dependency changes, run `make modtidy-all` and then `make check-mod-diff`. The first tidies the discovered Go modules; the second checks that go.mod and go.sum files have no unreviewed changes.
- `make check-component-metadata` validates component metadata using the generated metadata analyzer.
- `make bundle-component-metadata` checks that the component metadata bundle can be generated.
- `make prettier-check` checks JavaScript, TypeScript, and JSON formatting.

## Tests and local services

- Unit tests are next to the code and run through `make test`.
- The conformance harness is in [tests/conformance/](tests/conformance/) and its component definitions are in [tests/config/](tests/config/). Run it with `make conf-tests`. For a single test or local service setup, follow [tests/conformance/README.md](tests/conformance/README.md).
- Local service containers are defined in [.github/infrastructure/](.github/infrastructure/) using docker-compose-*.yml files. Start the compose file for the service under test before running its conformance case.
- Cloud-backed conformance cases need the matching provider resources and credentials. The workflow separates credential-backed cases from the PR-safe matrix; examples include Azure, AWS, GCP, Cloudflare, and RavenDB. Some conversation-provider cases also need API keys. Without those resources or variables, tests may skip or fail to connect; report that as skipped or unavailable, not as a passing run. See [.github/workflows/conformance.yml](.github/workflows/conformance.yml) and [tests/conformance/README.md](tests/conformance/README.md).
- Certification tests live in [tests/certification/](tests/certification/), which has its own go.mod. They are separate from `make test` and use the setup in [.github/workflows/certification.yml](.github/workflows/certification.yml); select the relevant component and provide its required services or credentials.

## Adding a component

Follow [docs/developing-component.md](docs/developing-component.md) for component structure, metadata, unit tests, and conformance registration. Prefer an existing component in the same building block as the starting point.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before making a change. Contributions are submitted as pull requests; sign each commit with `git commit -s` to satisfy the DCO, use [.github/pull_request_template.md](.github/pull_request_template.md), and reference the issue that defines the change.
