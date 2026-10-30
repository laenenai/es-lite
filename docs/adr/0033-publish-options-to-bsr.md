# ADR 0033: Publish `es/v1/options.proto` as a Buf Module

- **Status:** Accepted (configured; first push pending the `BUF_TOKEN` secret)
- **Date:** 2026-10-30

## Context

Every consumer of `protoc-gen-es-lite` must compile `es/v1/options.proto`
(the `sum_type` and `schema_version` extensions). `contracts` does it by
vendoring the file under `third_party/es/v1/options.proto` with a header
"Vendored from github.com/laenenai/es-lite … at commit a177f05. Do not edit;
re-vendor to update", and lists `third_party` as a second module in its
`buf.yaml` workspace. §13.2 asks for a Buf Schema Registry module instead.

The file's `go_package` is `github.com/laenenai/es-lite/gen/es/v1;esv1`, and
the plugin imports that exact package (`cmd/protoc-gen-es-lite/main.go`).
ADRs 0030–0032 add more options to the same file, which makes a copy-drift
between the two repos more likely, not less.

`buf.yaml` already declares the `proto` directory as a module (and
`examples/counter/proto` as a second one, which must stay private).

## Decision

1. Name the module: in `buf.yaml`, the `proto` module gets
   `name: buf.build/laenenai/es-lite`. The counter example module stays
   unnamed and is never pushed.
2. Push on release: `release.yml` runs `buf push proto --label <tag>` after the
   image build, authenticated with a `BUF_TOKEN` repository secret; `main`
   pushes with the `main` label from `ci.yml` so consumers can pin a commit
   before a tag exists. `buf lint` and `buf breaking --against
   '.git#ref=origin/main'` (already in `task lint:proto`) gate every push, so
   an extension number can never change silently.
3. The Go package path is **not** moved. `go_package` stays
   `github.com/laenenai/es-lite/gen/es/v1;esv1`; consumers import the Go
   types from the es-lite module, not from BSR-generated SDKs. In buf v2,
   dependencies are not generated unless `include_imports` is set, so a
   consumer's `buf generate` produces nothing for `es/v1`. Consumers using
   `managed` mode must add a `disable: { module: buf.build/laenenai/es-lite }`
   override for `go_package`.
4. `contracts` migration: add `buf.build/laenenai/es-lite` to `deps`, run
   `buf dep update`, delete `third_party/` and its module entry, and keep the
   same `import "es/v1/options.proto"` lines — the import path is the module's
   file path, unchanged.
5. Versioning: the BSR label equals the Git tag; `buf.lock` in consumers pins
   a commit. `es/v1` keeps the `v1` package for its life; new options are added
   with new numbers, never renumbered (the `breaking: FILE` rule in `buf.yaml`
   enforces it).

## Consequences

- Consumers stop vendoring; option additions (ADR 0030/0031/0032) reach them
  with a `buf dep update` instead of a manual copy, and `buf breaking` on
  their side sees the real history.
- The repository gains one secret (`BUF_TOKEN`) and one external account
  dependency. The BSR module is public, like the repository; a private BSR
  organisation is not required.
- The cold-start problem in `Taskfile.yml` (`generate:bootstrap`, where the
  plugin needs `gen/es/v1` before it can build) is unchanged — it concerns
  es-lite's own build, not consumers.
- The ADR 0028 note that "the `natsstore` JSON wire contract is local and
  unversioned; proto/versioning remains a follow-up" is unaffected: this ADR
  publishes the options file only.

## Alternatives Considered

### Keep vendoring with a checked-in commit hash

The status quo. Rejected: three new options are about to land, and a stale
vendored copy fails at generation time with an unhelpful "unknown extension"
rather than at dependency resolution.

### Publish a Go-module-only distribution (`gen/es/v1` as the contract)

Insufficient: `buf` needs the `.proto` to compile `contracts`' files that
reference `(es.v1.sum_type)`; the Go package alone does not help the
descriptor build.

### Move the options into `contracts` and have es-lite depend on it

Rejected for the dependency-direction reasons in ADR 0030.

## Implementation notes

- `buf.yaml` names the `proto` module `buf.build/laenenai/es-lite`; the
  counter fixture module stays unnamed.
- The push is a Taskfile task, `task buf:push [LABEL=…]`, gated by `buf lint`
  and `buf breaking` against `origin/main`'s `proto` subdirectory. `ci.yml`
  runs it with `LABEL=main` on pushes to `main`; `release.yml` runs it with the
  tag name after the image job. Both use `bufbuild/buf-action` with
  `setup_only` and the `BUF_TOKEN` repository secret.
- No push has been made from this branch: the BSR repository and the
  `BUF_TOKEN` secret must be created by the owner first. Until then
  `contracts` keeps its vendored copy; the migration steps in §4 are unchanged.
