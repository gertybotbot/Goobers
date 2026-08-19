# Kubernetes-native runner — phase 4 slice

**Status:** implemented, journal-derived durable notification publication
**Scope:** `api/v1alpha1/gooberrun_types.go`, `api/v1alpha1/gooberrunaction_types.go`, `internal/kuberunner`, `internal/notification`, `providers`, `cmd/goober-attempt`

This note records the authority boundaries the Kubernetes-native runner is built
on, and states explicitly what this slice does **not** do. It is deliberately
short. The full plan lives in the operator roadmap; this is the contract for the
code that actually exists today.

## 1. Why this exists

The local runner owns a run end to end inside one process. The Kubernetes-native
runner splits that: a leader-elected controller decides, and a Kubernetes Job
executes. Splitting decision from execution introduces exactly one new hazard —
**the executor can lie, vanish, or come back from the dead** — and everything in
this design is a response to it.

The single rule the runner enforces:

> A committed journal transition may cause execution. Execution never creates
> authoritative workflow state by itself.

## 2. Authority boundaries

| Surface | Role | May decide correctness? |
|---|---|---|
| Run journal (`events.jsonl`) | Authoritative workflow record | **Yes** |
| `GooberRun.status` | Fast projection for `kubectl`/API/portal | No |
| Kubernetes `Job` status | Execution evidence | No |
| Published result receipt | Evidence, admitted only after validation | No |
| `GooberRunAction` | Occurrence-bound request, not a decision | No |

Three consequences are implemented, not merely asserted:

**The journal is read first, always.** `Reconcile` derives a `JournalHead` from
committed events before it looks at anything else. No decision reads CR status
as an input. A controller crash loses nothing, because it kept nothing.

**Status is rebuildable.** `ProjectStatus` is a pure function of the journal
head. Deleting the whole status and reconciling reproduces it exactly, and
writes zero journal events while doing so. This is pinned by
`TestStatusRebuildsExactlyFromTheJournal`.

**A Job completing advances nothing.** The controller fetches the attempt's
result receipt, validates it, and only then commits `stage.finished`. A Job that
exited 0 without publishing a usable result leaves the run exactly where it was.
"The process exited 0" and "the stage decided this" are different claims, and
only the second one may move a run.

## 3. Durable business claims are not leader election

An item-triggered run (a pinned repository with `externalId`) must acquire a
business claim before it may dispatch. Claim identity is the full
`(gaggle, provider, external ID)` tuple. The authority is a retained ConfigMap
record named by a hash of that tuple in one configured authority namespace
(`goobers-system` by default), updated with Kubernetes `resourceVersion`
compare-and-swap. Centralizing the records keeps runs in different namespaces
from acquiring parallel copies of one business claim. It is deliberately **not**
a `coordination.k8s.io/Lease`:
leader-election leases answer whether a controller process is live; a business
claim answers which immutable run occurrence may act on a provider item.

Each grant carries a positive, monotonically increasing fencing epoch. Released
records are retained, and expiry/reacquisition increments rather than resets the
epoch. Acquisition by the same `(run ID, run UID)` is idempotent and renews the
same epoch, which is how a controller restart recovers without minting a second
authority. Acquisition by another live holder is refused. Renewal, assertion,
and release all compare key, run ID, run UID, epoch, and lease expiry; a stale
worker fails closed.

The claim grant is then appended to the run journal as `claim.acquired` before
any `stage.started`. That event is the run's durable record of which fence it
was granted. Reconciliation reconstructs it from the journal and renews the
external claim record before every executable decision. Terminal ordering is
the reverse: `run.finished` commits first, the claim is released second, and
`claim.released` records that disposition last. Release is idempotent for the
same retained epoch, closing the crash window between release and journalling
the disposition.

This split is intentional: the retained ConfigMap provides atomic contention
between runs, while the append-only run journal remains workflow and recovery
authority. `GooberRun.status` participates in neither decision.

## 4. Source-backed execution and the mutation fence

An executable Job receives one closed, immutable `AttemptPlan` projected from
its run's pinned `CompiledMachine`. The plan binds the exact attempt identity,
invocation envelope, task or agentic-gate source, and declared capabilities. It
contains no credential values. Capability-to-Secret bindings are added to the
Pod separately, and only for grants present in that plan; two grants that try to
claim one environment variable are refused rather than silently overwriting one
another.

The worker invokes the existing neutral abstractions rather than adding another
execution engine: agentic tasks and reviewer gates use `invoke.Goober`, while
trusted in-process provider tasks use `invoke.Deterministic`. The worker writes
only a result receipt. It never appends a transition or chooses the next state;
the controller remains the sole workflow authority.

The capability audit's split is enforced at this boundary. Read/model authority
may be handed to an agentic reasoner, but an agentic plan carrying a repository
or provider mutation grant is refused before `stage.started` and before any
credential or Job exists. Mutation-capable deterministic execution must
explicitly implement `FencedProviderExecutor`. Its provider context installs a
`providers.MutationFence`, checked immediately before every non-read GitHub,
Gitea, or Azure DevOps HTTP request. A stable attempt-bound idempotency key
accompanies that check. Shell subprocesses and unmarked executors cannot claim
that they are fenced. Every mutation-capable plan also requires a positive
business-claim epoch; a claimless mutation is refused before dispatch.

`JournalAttemptAuthority` re-reads the journal before execution, every provider
mutation, and result publication. The run must still have the exact open
`(state, branch, attempt)` occurrence, and an item-triggered attempt's journal
claim and retained ConfigMap epoch must both match. Terminal/cancelled runs,
taken-over claims, and superseded attempts fail closed before side effects. A
second assertion rejects a completion that became stale while its executor was
running; the controller independently performs the final assertion before
journal acceptance.

Result slots are immutable. Re-publishing identical bytes after a worker restart
is idempotent; replacing an already-published outcome is refused. A restarted
worker that finds a valid receipt does not invoke its agent/provider again.

## 5. Deterministic attempt identity and journal-derived numbering

The dangerous window is between *decide to dispatch* and *Job exists*. It is
closed by ordering plus determinism:

1. append `stage.started` (dispatch intent) to the journal;
2. create the Job at a name derived from durable facts.

`JobName` is a pure function of `(run UID, state, branch, attempt)`:

```
gr-<runhash>-<statehash>-a<attempt>
gr-<runhash>-<statehash>-b<branch>-a<attempt>
```

A crash anywhere in that window is recoverable: the next reconcile recomputes
the same name and creates it, and `AlreadyExists` is treated as **success**, not
as an error. So an attempt has exactly one Job, whether the reconcile ran once,
eight times, or resumed after a crash.

Two details worth stating because they are easy to get wrong:

- The name derives from the **Kubernetes UID**, not the run ID. A `GooberRun`
  deleted and recreated under the same name is a different occurrence and must
  not adopt its predecessor's Jobs.
- The state name is **hashed**, not truncated into the name. State names are
  author-controlled free text; a truncated or character-substituted identifier
  looks authoritative and is not. `goobers.dev/state` is set only when the raw
  name is already a legal label value, and the reconciler never depends on it.

Attempt numbers come exclusively from replaying `stage.started` events in the
journal head. The highest attempt for `(state, branch)` plus one is the next
number. Deleting status, restarting the controller, or restoring a stale status
object therefore cannot reuse an attempt number. The full `status.attempts`
list is also reconstructed from those events, including `dispatchedSeq`; it is
not carried forward from old status.

## 6. Retry belongs to the controller

Every attempt Job is created with `backoffLimit: 0` and `restartPolicy: Never`.

This is a correctness requirement, not a tuning choice. A Kubernetes-initiated
restart would be a second execution sharing one attempt identity — two processes
entitled to publish into the same result slot, with one journal record covering
both. Instead, each retry is a separate controller decision, a separate
journalled attempt, and a separate deterministically-named Job.

## 7. The result-publication contract

A Job publishes a `ResultReceipt`. The controller refuses to journal anything
until the receipt passes, in this order:

1. **Schema** — the receipt version is one this build owns; unknown fields are
   refused rather than ignored.
2. **Address and fence** — the receipt names the exact attempt
   `(run UID, run ID, state, branch, attempt, fencing epoch)` that was
   dispatched. This is what
   stops a stale worker from an earlier attempt writing into the live slot and
   being read as the current attempt succeeding.
3. **Digest** — the envelope bytes are re-hashed and compared to the digest the
   receipt commits to. The envelope travels as raw bytes precisely so this is
   checkable; re-encoding a decoded struct is not guaranteed to reproduce the
   producer's bytes, and a digest you cannot reproduce is decoration.
4. **Content** — the result status is known, and every artifact pointer is
   digest- and containment-validated *before* it reaches the journal.

Only `ValidateResult` can construct a `ValidatedResult`, and only a
`ValidatedResult` can be passed to `AppendStageFinished`. "Did we validate this?"
is answered by the type system rather than by remembering to call a checker.
Immediately before accepting that validated result, the controller reasserts
that the attempt's claim epoch is still current. Thus even a well-formed late
receipt from a worker whose lease expired cannot cross the journal mutation
seam. Provider effects use the same authority through `providers.MutationFence`
before each non-read request, so result and side-effect admission cannot drift
onto different fencing rules.

Any failure is a **hard stop**: the run does not advance. Missing is
distinguished from invalid, because conflating them either wedges a run forever
or advances it on garbage. A zero-length file counts as missing, so a
half-written publish costs a requeue rather than an attempt.

Transport is a `ResultReader` interface; today it is a file on a shared volume.
Swapping in an artifact store or broker later changes no correctness property,
because none of the validation depends on where the bytes came from.

## 8. Durable notification publication and advisory wake gossip

Human-pause and terminal notifications are projections of committed journal
occurrences. `JournalNotificationProjector` scans the canonical event log in
sequence order and renders the existing provider-neutral
`NotificationRequest` contract from `gate.paused` and `run.finished`. The
request identity and idempotency key are deterministic functions of
`(run ID, source journal sequence)`, and its expiry is derived from the source
event time rather than controller wall-clock time.

The request is appended as `notification.requested` to the same run journal.
`EnsureNotificationRequested` rechecks the exact source occurrence while it
owns the journal writer lock and atomically returns an existing publication or
appends the first one. A controller crash before, during, or after publication
therefore converges on one durable request. The first rendering wins: changing
sink selection or TTL later cannot rewrite output history. Notification events
remain excluded from workflow conformance and `HeadFromEvents` never consumes
them to choose a state, attempt, terminal result, or claim disposition.

Only after the request is durable may the projector emit a `WakeHint`. The hint
contains run ID, Kubernetes run UID, source sequence, and publication sequence;
it contains no workflow transition, result, claim token, or rendered message.
Receivers use it only as a doorbell and must reread the journal. `WakeCoalescer`
drops duplicate and stale hints by publication sequence and keys its high-water
by run UID, so delayed gossip for a deleted/recreated CR cannot suppress its
replacement. Gossip errors are diagnostic only: they neither roll back the
request nor affect reconciliation. Replaying after restart may deliberately
repeat the doorbell because gossip is lossy and advisory.

The deployment selects registered sink kinds through operator options. This
slice stops at the durable journal publication and transport-neutral wake seam;
it does not invent a Kafka/Nisshi or NATS client where the repository has no
production adapter. The existing `internal/notification` dispatcher remains the
bounded, receipted sink-delivery boundary.

## 9. Human gates

A human gate executes no process, so it gets **no Job**. The controller appends
`gate.paused` and projects phase `Waiting` with the pause occurrence. Parking is
idempotent: repeated reconciles do not append a `gate.paused` per pass.

`Waiting` is an operational refinement of *running*, not a journal phase. It must
never be read as a terminal outcome.

## 10. Occurrence-bound actions

Human actions arrive **late** by nature — a notification sits in a chat client
and someone clicks an hour later. By then the run may have moved on, re-entered
the same gate under a new occurrence, or been deleted and recreated. Under
name-based addressing every one of those resolves the wrong decision onto the
wrong occurrence, and looks entirely normal afterwards.

So a `GooberRunAction` must quote **both**:

- the target run's Kubernetes **UID**, and
- the exact journal **sequence** of the pause or terminal event it answers.

Both are checked against the journal (not against status, which may lag).
Mismatches are rejected with stable reasons: `StaleOccurrence`,
`RunUIDMismatch`, `RunNotFound`, `Invalid`. A gate *name* is never sufficient
identity, because one gate can pause many times in a single run.

`Applied` and `Rejected` are terminal, so a requeue cannot journal a decision
twice.

## 11. Immutability

Both CRD specs are immutable via CEL `self == oldSelf`.

- `GooberRunSpec` is pinned identity. A config reload must not retune a run in
  flight, so the pinned workflow digest, provider routing, and run controls
  cannot be edited after creation.
- `GooberRunActionSpec` is a one-shot **fact** ("this actor decided this, for
  this occurrence"), not desired state. Editing one in place would rewrite
  history.

Provider kind lives in the pinned spec deliberately: the live Gitea incident was
caused by terminal/cleanup work routing to the wrong provider. A run must not be
able to discover its provider from mutable config at a terminal seam.

## 12. Explicit non-goals for this slice

These are **absent, not stubbed**. Nothing in the tree should be mistaken for a
partial implementation of any of them.

- **No direct agent mutation authority.** The audit found no sound whole-stage
  deterministic conversion. Agentic reasoning remains supported, but write
  grants stay withheld until each workflow emits a bounded intent consumed by a
  trusted fenced provider executor. This slice supplies and enforces that
  executor seam; it does not silently hand raw write credentials back to the
  model process.
- **No new workflow or mutation authority.** Attempt plans, Jobs, provider
  idempotency keys, and result receipts are evidence. The journal and retained
  business claim remain the only authorities; there is no mutation CRD or
  second state machine.
- **No invented broker adapter.** Durable notification requests use the existing
  journal and notification contracts. The wake interface and stale-ordering
  coalescer are ready for a real deployment adapter, but this slice does not
  pretend that a test double is a Kafka/Nisshi producer or NATS client.
- **No Temporal and no PostgreSQL.** `internal/engine` stays quarantined.
  Registering the runtime CRDs in the operator scheme does not activate it; the
  two runners share a scheme and nothing else. The native runner's only durable
  dependency is the same plain-file journal the local runner already owns.
- **No live-cluster canary.** Verification is unit tests and fake-client
  reconciles against a real on-disk journal.
- **No parallel/fan-out execution.** `@join` ends a *branch*, not a run, so it is
  reported `StateUnknown` rather than being mistaken for a terminal that would
  silently finish a run which should have fanned back in.
- **No automated-gate evaluation.** An automated gate parks with a stable
  `AutomatedGateUnsupported` reason rather than being silently treated as a pass.
- **No changes to local runner semantics.** The local runner was not modified to
  make the controller easier. The canonical event taxonomy and runner
  conformance expectations are unchanged.

## 13. What is verified

- deterministic Job identity, including separation across runs, states,
  branches, attempts, and recreated CRs;
- create-once under repeated reconciles and under a simulated crash between
  journal append and Job create;
- `backoffLimit: 0` and `restartPolicy: Never`;
- valid result commits `stage.finished` then `run.finished`, in that order, and
  projects `Completed`;
- Job success with missing / garbage / misaddressed / digest-mismatched result
  advances nothing;
- a human gate creates no Job and projects `Waiting` with a pause occurrence;
- status deleted and rebuilt from the journal repairs exactly, writing no events;
- status deletion between retries still dispatches the journal-derived next
  attempt and reconstructs both attempt refs;
- concurrent claim refusal, same-run restart recovery, expiry takeover with a
  strictly larger epoch, idempotent terminal release, and stale-token refusal;
- source-backed agentic execution through `invoke.Goober` and provider execution
  through a marked `invoke.Deterministic` executor;
- plan/config propagation and capability-scoped Secret wiring without an
  undeclared credential leak;
- claim identity and epoch propagation into the Job and result receipt;
- direct mutation-capable agent plans and unmarked provider executors are
  refused before dispatch or side effects;
- a stale/cancelled worker is refused immediately before provider HTTP effects,
  and a worker that loses authority while executing cannot publish completion;
- a stale worker's otherwise valid receipt cannot commit `stage.finished` after
  another run owns the next fencing epoch;
- worker restart reuses an immutable matching result, conflicting publication is
  rejected, and controller retry remains one separately-journalled Job per
  attempt;
- stale `GooberRunAction` occurrence and run-UID are rejected with stable
  reasons;
- pause and terminal notification requests are derived in journal order,
  survive controller restart without duplicate publication, and retain their
  first durable rendering across config changes;
- wake gossip happens only after durable publication, gossip failure cannot
  invalidate it, and duplicate/stale hints are coalesced per run UID;
- the on-disk journal store and result transport round-trip against a real
  journal.

## 14. Known gaps

- A Job that completes and never publishes is surfaced via a `ResultMissing`
  condition, but there is no timeout that converts it into a failed attempt yet.
- `ttlSecondsAfterFinished` is not set: Jobs are retained because result and log
  retention are not yet proven.
- No Kafka/Nisshi sink or NATS transport is wired. Adding one must use its real
  client and authentication/configuration model; wake payloads remain advisory
  and durable sink outcomes continue to use notification delivery receipts.

## 15. Generator hazards worth remembering

Two controller-gen behaviours cost real debugging time here and will bite again:

- **`uint64` is not a representable Kubernetes API type.** The journal counts
  sequences as `uint64`, but a `uint64` field panics inside
  structured-merge-diff on any status write. Every sequence crossing the API
  boundary is `int64`, narrowed by `projectSeq`.
- **A bare `''` in a CEL marker does not survive generation.** controller-gen
  rendered `self.rationale != ''` into the YAML with a Unicode right double
  quote, which fails CEL compilation and makes the apiserver reject the entire
  CRD at install time. The rules use `size(x) > 0` instead. This was invisible
  to every unit test and was caught only by the envtest run, which is why that
  test installs the real CRDs rather than trusting the Go types.

Note also that `config/crd/bases` currently contains two CRDs
(`goobers.goobers.dev`, `workflows.goobers.dev`) that a 1.31 apiserver refuses
for exceeding the CEL cost budget. That is pre-existing and outside this slice,
but it is why the envtest here installs only the two runtime CRDs by name.
