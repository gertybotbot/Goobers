# Kubernetes-native runner — phase 1 slice

**Status:** implemented, first vertical slice
**Scope:** `api/v1alpha1/gooberrun_types.go`, `api/v1alpha1/gooberrunaction_types.go`, `internal/kuberunner`

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

The single rule the whole slice enforces:

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

## 3. Deterministic attempt identity

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

## 4. Retry belongs to the controller

Every attempt Job is created with `backoffLimit: 0` and `restartPolicy: Never`.

This is a correctness requirement, not a tuning choice. A Kubernetes-initiated
restart would be a second execution sharing one attempt identity — two processes
entitled to publish into the same result slot, with one journal record covering
both. Instead, each retry is a separate controller decision, a separate
journalled attempt, and a separate deterministically-named Job.

## 5. The result-publication contract

A Job publishes a `ResultReceipt`. The controller refuses to journal anything
until the receipt passes, in this order:

1. **Schema** — the receipt version is one this build owns; unknown fields are
   refused rather than ignored.
2. **Address** — the receipt names the exact attempt
   `(run UID, run ID, state, branch, attempt)` that was dispatched. This is what
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

Any failure is a **hard stop**: the run does not advance. Missing is
distinguished from invalid, because conflating them either wedges a run forever
or advances it on garbage. A zero-length file counts as missing, so a
half-written publish costs a requeue rather than an attempt.

Transport is a `ResultReader` interface; today it is a file on a shared volume.
Swapping in an artifact store or broker later changes no correctness property,
because none of the validation depends on where the bytes came from.

## 6. Human gates

A human gate executes no process, so it gets **no Job**. The controller appends
`gate.paused` and projects phase `Waiting` with the pause occurrence. Parking is
idempotent: repeated reconciles do not append a `gate.paused` per pass.

`Waiting` is an operational refinement of *running*, not a journal phase. It must
never be read as a terminal outcome.

## 7. Occurrence-bound actions

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

## 8. Immutability

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

## 9. Explicit non-goals for this slice

These are **absent, not stubbed**. Nothing in the tree should be mistaken for a
partial implementation of any of them.

- **No durable claims and no fencing epochs.** There is no claim acquisition,
  renewal, or release here. Kubernetes `Lease` objects are for controller leader
  election and liveness only — they are *not* business-claim authority, and this
  slice does not pretend otherwise by introducing one.
- **No provider mutation broker.** No scoped write tokens, no idempotency
  receipts, no epoch validation. Agentic and provider-writing stages are not
  safe on this path yet.
- **No Nisshi / Kafka broker client.** No outbox, no topics, no consumer groups.
- **No NATS.** No advisory subjects, no gossip, no wake-up hints.
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

## 10. What is verified

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
- stale `GooberRunAction` occurrence and run-UID are rejected with stable
  reasons;
- the on-disk journal store and result transport round-trip against a real
  journal.

## 11. Known gaps

- `nextAttemptNumber` counts attempts from projected status. Status is a
  projection, so an undercount is possible in principle; it is currently
  self-correcting because the deterministic Job name collides and create-once
  absorbs it. Deriving the attempt number from the journal head is the cleaner
  fix and belongs in the next slice.
- A Job that completes and never publishes is surfaced via a `ResultMissing`
  condition, but there is no timeout that converts it into a failed attempt yet.
- `ttlSecondsAfterFinished` is not set: Jobs are retained because result and log
  retention are not yet proven.

## 12. Generator hazards worth remembering

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
