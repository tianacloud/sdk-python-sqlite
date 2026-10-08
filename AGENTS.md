# Python SQLite adapter boundary (2026-09-28)

User requires the same architecture as the Rust SDK split: generic sdk-python
transport, this repository for the dependent SQLite SDK. No compatibility or
migration work. Canonical remote was empty; task branch is unborn until an
explicitly authorized initial commit. Do not commit, push or publish without request.

Own the hrana-http profile, Hrana HTTP v3 pipeline, parameters/results, rotating
baton, transactions and session lifecycle here. Depend on tiana-sdk rather than
copying transport code or embedding an engine/helper. References: sdk-go,
sdk-go-sqlite and the companion Rust implementation; App/Gateway/Control and
specification sources were consulted read-only during the Rust split.

A Session owns one lazy tunnel from a caller-owned Client. Reject concurrent
operations with SESSION_BUSY; no mutex queue or hidden retry. Session state becomes
unusable and autocommit unknown before network awaits. Restore only after a fully
validated pipeline reply; timeout/cancellation/uncertainty releases the tunnel and
never reconnects a baton or replays SQL. Propagate CancelledError, preserve an
original exception during context-exit cleanup, and release transport tasks with
bounded close. Explicit abort affects only this session and cancels its owner task.
Python GC is not async cleanup: require context management or aclose().

Provide execute/query, begin/commit/rollback, raw transaction SQL/savepoints,
execute-and-close and explicit close. Fetch get_autocommit with each statement.
Known SQL errors may have partial effects or keep a transaction active. Failed
commit does not imply rollback. Server close rolls back uncommitted work; transport
loss alone does not: baton/locks may live until server TTL. Storage integrity,
atomicity, durability, crash recovery and locking remain server-owned. Never claim
cancellation or an outcome_unknown=false SQL error means no side effects.

Bound wire bodies to 8 MiB, headers to 32 KiB, batons to 4096 UTF-8 bytes; preserve
int64, finite floats, UTF-8 and strict unpadded base64. Snapshot/encode arguments
before awaiting. HTTP via h11 has no redirects, retries, decompression or inner
auth forwarding. Strict JSON rejects duplicate fields/nonfinite constants and
requires matching result/row shapes. Only allowlisted codes and generated request
IDs appear in errors; never include SQL, data, batons or remote error messages.
Default request timeout 30 s; close <=3 s plus bounded transport cleanup. Buffered
results trade simple lifetime semantics for memory proportional to decoded rows
(which is larger than wire size). No pool, DB-API/ORM adapter or streaming API.

The core dependency is the immutable GitHub source requirement recorded below;
wheel metadata must preserve it and installers fetch the core remotely. No local
core wheels, source-path imports or editable core installs are required. This
changes no server wire or on-disk format and adds no compatibility shim.

Verify core and adapter tests, minimum Python 3.11, lint, wheel/sdist construction,
installed-wheel operation without source-path imports, and actual App SQLite SQL
against a disposable loopback database. Include cancellation/no replay, simultaneous
call rejection, invalid input preserving transactions, response limits/types,
known/unknown errors, commit/rollback/savepoints, isolation and close rollback.
No production cluster, S3 durability or crash-recovery claims from local fixtures.


## 2026-09-28: verified remote Tiana dependencies

User requires Tiana SDK dependencies to resolve from current GitHub main commits,
never sibling paths or unpublished local builds. Pin verified immutable commits:
sdk-go e69b9c1d3842985e0ba9fd98b2403c520e5f98f1;
sdk-go-sqlite ce8df62d600c6509ef7a3308b17603e68f8c73d7;
sdk-rust f45f14e36313e1ec5787e212de9650c16c9f3067;
sdk-python 940c26c00b9f3abec63537388c4a23b0a129a98a;
sdk-node ba52cb6ec4e751f5158b17d64b45323a82f74f81.
Go records canonical resolved versions and go.sum with GOWORK=off; Rust uses Git
rev and Cargo.lock; Python uses a PEP 508 immutable Git requirement preserved in
wheel metadata; Node uses a Git dependency and package-lock. No local replace,
path patch, workspace link, editable core package or local core tarball fallback.
This supersedes earlier unpublished-core/local-development dependency guidance.

Tradeoff: reproducible builds need GitHub HTTPS access; no SSH key is required.
Future main changes require an explicit pin refresh. Verify fresh
resolution in isolated source copies without sibling repositories, inspecting
module/Cargo/installed Python/npm source metadata, plus relevant tests and package
consumers. Preserve wire, transaction, no-replay, TLS and storage invariants; this
update adds no runtime network round trips or persistence format migration.
No third-party version refresh is intended. Revert manifests and locks together
for rollback. Commit/push/package publication is outside this dependency update.


## 2026-09-28: runnable Python SQLite example

User requires examples in this repository. Provide examples/sqlite.py plus its
README, included in the sdist. Import installed SDK packages without path changes;
core dependency remains the pinned remote Git commit. Accept explicit environment
configuration: TIANA_ENDPOINT, optional TIANA_TOKEN/PEM TIANA_CA_FILE and physical
TIANA_GATEWAY_ADDRESS with bracketed IPv6. Do not add implicit env reading to the
library, retired env aliases, token-file readers or disabled TLS verification.
Public demo instructions use example.test rather than internal deployment domains.

Use only a connection-local TEMP table and bound typed values, demonstrate commit,
rollback and named queries, close Session then Client through context managers.
No retries/replay, persistent table modifications or automatic credential refresh.
Report bounded SDK codes, not credentials or remote messages. Cleanup on errors
preserves the SDK's uncertain-outcome semantics; failed close/cancellation cannot
promise rollback or immediate server cleanup. No API/wire/storage format change;
resource limits stay those of Session. Remove example/docs together to roll back.
Validate minimum Python 3.11, lint/format, source distribution contents, installed
wheel imports with remote core, invalid config before CONNECT, and two real App
runs proving TEMP isolation, typed values and rollback. Preserve existing work.


## 2026-09-28: HTTPS Git dependency

User requires the core SDK Git dependency to use HTTPS, not SSH. Keep the pinned
commit unchanged; synchronize the package manifest, lockfile (where present),
README and built package metadata. No credentials in URLs, SSH/local fallback,
or URL rewrite is allowed to mask validation. Anonymous HTTPS accessibility must
be verified separately; if authentication is required, use HTTPS credentials.
This changes dependency download transport only, with no runtime, wire, storage,
transaction or performance impact. Roll back manifests and lockfiles together.
Verify package metadata and lock consistency, and report any remote-fetch failure
without claiming a successful install. Preserve all existing uncommitted work.


## 2026-09-28: published core fixes dependency refresh

User explicitly authorized commit/push of the three core SDK repositories, then
updating their SQLite adapter dependencies. sdk-python main was pushed and read back
at c4c1b496ddddb13c2a387844c0e146bceb06aa0d. Pin this exact immutable remote Git HTTPS revision in the manifest,
README and lockfile where present. Preserve existing adapter implementation,
examples and editor swap files. No local path/link/editable-core dependency and
no third-party version refresh. This brings the opaque credential fix into this
adapter; Rust additionally receives the PEM CA builder API. Existing transport,
no-replay, transaction, storage and resource-bound contracts remain unchanged.
Rollback means restoring the previous pin and corresponding lock/doc together;
it also restores old fixed-format token rejection. Validate installed packages
against the remotely retrieved revision, with source provenance checked, plus
relevant native tests/types/builds. Direct HTTPS authentication is unavailable on
this machine; separately identified remote-transfer validation may use existing
SSH authentication with a process-scoped Git rewrite, never a product dependency
fallback or a claim of anonymous HTTPS success. The production URL remains HTTPS.
Only core repositories were authorized for commit/push; adapter changes remain
uncommitted for review. No SQLite adapter publication is authorized.

## 2026-10-08: v1.0.0 dependency and release verification

User authorizes dependency repair, installed-package and demo validation, then
squashing this repository to one commit and publishing main plus v1.0.0 only.
Use the core v1.0.0 release (Go module version; immutable Git HTTPS revision for
Node/Rust/Python), superseding the old historical pins above. Preserve third-party
versions, TLS verification, no SQL replay and transaction/storage semantics.
Use a disposable local App SQLite for demo verification; never production data.
