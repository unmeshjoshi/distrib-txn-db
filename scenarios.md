# HLC + Snapshot Isolation Scenarios (YugabyteDB-style)

Scope: scenarios required to fully demonstrate HLC-based Snapshot Isolation as implemented in YugabyteDB's DocDB layer. Out of scope: Raft replication of tablets/status tablet, Serializable isolation (read intents), wait-queues / deadlock detection, follower reads, secondary indexes.

Each scenario lists: the invariant under test, the setup, the sequence, the assertions, and the existing test (if any) that already covers it. Scenarios marked **GAP** are not yet implemented in this repo.

---

## 1. HLC mechanics (foundational)

These usually live in module `01-hybrid-clock`, but are listed here for completeness — they are the substrate on which everything else rests.

### 1.1 HLC monotonicity at a single node
- **Invariant**: `HLC.now()` is strictly monotonic even under wall-clock stalls.
- **Setup**: One node, no traffic.
- **Steps**: Freeze wall clock at T; call `now()` repeatedly.
- **Assert**: Each call returns a strictly greater `(physical, logical)` tuple; logical counter advances when physical is frozen.

### 1.2 HLC max-update on inbound message
- **Invariant**: `local = max(local, received).next()` on any inbound RPC.
- **Setup**: Node A at physical T1, node B at physical T2 > T1.
- **Steps**: B sends any RPC to A.
- **Assert**: A's HLC after receive ≥ T2.

### 1.3 HLC propagation across multi-hop RPC chain
- **Invariant**: HLC is transitive across forwarded RPCs.
- **Setup**: Three nodes A → B → C; A's clock leads, C's clock lags.
- **Steps**: A sends RPC to B, B forwards a derived RPC to C.
- **Assert**: C's HLC after receive ≥ A's HLC at send.

---

## 2. MVCC storage with intent / committed split

The DocDB-style separation of provisional records (intents) from committed versions.

### 2.1 Intent written to intent store is invisible in committed store
- **Invariant**: Intents are physically isolated from committed reads.
- **Setup**: Single replica.
- **Steps**: Begin txn, write key, do **not** commit.
- **Assert**: `intentStore` has the intent at the provisional HLC; `committedStore.getAsOf(key, ts(LARGE))` is empty.
- **Existing**: `TransactionalStorageReplicaCoreFlowTest.txnWriteStoresIntentAndReadReturnsOwnIntent`.

### 2.2 Commit moves intent to committed store (apply path)
- **Invariant**: After commit, intent is deleted and a committed version exists at the commit timestamp.
- **Setup**: Single replica.
- **Steps**: Begin, write, commit; wait for resolve to settle.
- **Assert**: `committedStore.getAsOf(key, commitTs)` returns the value; `intentStore.getAsOf(key, LARGE)` is empty.
- **Existing**: `TransactionalStorageReplicaCoreFlowTest.commitMovesIntentToCommittedStoreAndMarksTransactionCommitted`.

### 2.3 Multiple committed versions of same key are stored with distinct HLCs
- **Invariant**: MVCC retains older versions; reads at older snapshots see older values.
- **Setup**: Pre-seed committed store with `(key, "v1", ts(900))`, `(key, "v2", ts(1100))`.
- **Steps**: Begin txn at snapshot ts(1000); read key.
- **Assert**: Returns `"v1"`. Begin a second txn at snapshot ts(1200); read returns `"v2"`.
- **GAP** (partially covered by `txnReadCommittedValuesAtReadTimestamp`).

### 2.4 Read at snapshot HLC returns max committed version ≤ snapshot
- **Invariant**: Snapshot reads obey "as-of" semantics.
- **Setup**: Two committed versions at ts(900) and ts(1100).
- **Steps**: Begin txn at snapshot ts(1000); read.
- **Assert**: Returns the ts(900) value, not ts(1100).
- **GAP**.

---

## 3. Single-transaction lifecycle

### 3.1 Begin creates a PENDING txn record on the coordinator
- **Invariant**: The status tablet (coordinator) holds an authoritative PENDING record from begin onward.
- **Setup**: Three nodes.
- **Steps**: Client calls `beginTransaction(txnId, SNAPSHOT)`.
- **Assert**: `replicaFor(coordinatorFor(txnId)).txnRecords().get(txnId).status() == PENDING`; isolation level is SNAPSHOT; heartbeat timeout is ticking; `participantReplicas` is empty.
- **Existing**: `TransactionalStorageReplicaCoreFlowTest.beginTransactionCreatesPendingTxnRecordOnCoordinator`.

### 3.2 Write-then-read inside same txn sees own intent
- **Invariant**: Read-your-own-writes within a transaction.
- **Setup**: Single replica.
- **Steps**: Begin, write `(key, "v")`, read `key`.
- **Assert**: Read returns `"v"` even though no commit has occurred and no committed version exists.
- **Existing**: `TransactionalStorageReplicaCoreFlowTest.txnWriteStoresIntentAndReadReturnsOwnIntent`.

### 3.3 Commit flips status COMMITTED and records commit timestamp
- **Invariant**: Status flip is the linearization point; commit timestamp ≥ all intent timestamps and ≥ coordinator's HLC at prepare.
- **Setup**: Single key txn.
- **Steps**: Begin, write, commit.
- **Assert**: `TxnRecord.status == COMMITTED`; `commitTimestamp ≥ intentTimestamp`; `participantReplicas` equals the set the client touched.
- **Existing**: `TransactionalStorageReplicaCoreFlowTest.commitMovesIntentToCommittedStoreAndMarksTransactionCommitted`.

### 3.4 Read at snapshot earlier than commit timestamp does not see the commit
- **Invariant**: Snapshot isolation — a txn cannot observe commits that happen after its snapshot.
- **Setup**: One client commits a value at ts(1100). A second client started at snapshot ts(1000) reads the key.
- **Steps**: Standard.
- **Assert**: Second client's read sees the pre-commit state (no value, or an older committed version).
- **GAP**.

---

## 4. Read-path intent resolution (4 status cases)

When a read encounters another transaction's intent, the participant queries the status tablet and acts on the response.

### 4.1 Intent's txn is PENDING → ignore intent, return committed-below
- **Invariant**: A reader never blocks on or observes uncommitted data.
- **Setup**: Pre-seed: committed `(key, "750", ts(900))`; intent `(key, "1000", ts(1100))` belonging to a PENDING txn.
- **Steps**: New txn at snapshot ts(1200) reads `key`.
- **Assert**: Returns `"750"`; intent untouched in intent store.
- **Existing**: `TransactionalStorageReplicaReadResolutionTest.txnReadIgnoresIntentFromOtherPendingTransactionsAndReturnsCommittedValue`.

### 4.2 Intent's txn is COMMITTED at commitTs ≤ snapshot → materialize, then read
- **Invariant**: Reader drives forward unresolved-but-committed intents and observes them.
- **Setup**: Pre-seed committed txn record (`COMMITTED`, commitTs ts(1200)); intent at provisional ts(1100) still in intent store.
- **Steps**: New txn at snapshot ts(1300) reads `key`.
- **Assert**: Read returns the intent's value; intent moved to committed store at commitTs; intent deleted.
- **Existing**: `TransactionalStorageReplicaReadResolutionTest.txnReadResolvesCommittedIntentsFromOtherTransactionsBeforeReturningValue`.

### 4.3 Intent's txn is COMMITTED at commitTs > snapshot → ignore intent, return committed-below
- **Invariant**: A committed write whose commit time is after my snapshot is invisible to me.
- **Setup**: Pre-seed committed `(key, "750", ts(900))`; committed txn record at ts(1500); intent at ts(1100).
- **Steps**: New txn at snapshot ts(1200) reads `key`.
- **Assert**: Returns `"750"`; intent may be resolved into committed store (visible at commit time, not at this snapshot).
- **GAP**.

### 4.4 Intent's txn is ABORTED → delete intent, return committed-below
- **Invariant**: Aborted intents are garbage-collected by the reader path.
- **Setup**: Pre-seed committed `(key, "750", ts(900))`; aborted txn record; intent at ts(1100).
- **Steps**: New txn at snapshot ts(1200) reads `key`.
- **Assert**: Returns `"750"`; intent deleted; no committed version added.
- **GAP**.

---

## 5. Write-path intent resolution (4 status cases)

A writer attempting to write a key with an existing intent must consult the status tablet.

### 5.1 Intent's txn is PENDING → conflict
- **Invariant**: Concurrent writes to the same key under SI cannot both proceed; one must wait or fail.
- **Setup**: Two txns. T1 writes intent on `key`. T2 attempts to write `key`.
- **Steps**: T1 begin, T1 write, T2 begin, T2 write.
- **Assert**: T2 write fails with `"Conflicting pending transaction"` (this repo's fail-on-conflict policy; YB defaults to wait-on-conflict with priority push, which would be modeled differently).
- **Existing**: `TransactionalStorageReplicaWriteResolutionTest.txnWriteFailsWhenIntentFromOtherTransactionIsStillPending`.

### 5.2 Intent's txn is COMMITTED at commitTs ≤ my snapshot → resolve, then write
- **Invariant**: A writer drives forward an unresolved committed intent if it's not a SI violation.
- **Setup**: A COMMITTED txn left an unresolved intent at commitTs(1100). New writer's snapshot is ts(1300).
- **Steps**: New txn write to same key.
- **Assert**: Lingering intent is resolved into committed store; new write succeeds (and creates a new intent at the new txn's provisional HLC).
- **Existing (close)**: `TransactionalStorageReplicaWriteResolutionTest.txnWriteResolvesLingeringCommittedIntentAfterDroppedResolveRequest` — uses a dropped resolve RPC to construct exactly this state.

### 5.3 Intent's txn is COMMITTED at commitTs > my snapshot → first-committer-wins, fail
- **Invariant**: SI's defining lost-update prevention.
- **Setup**: A COMMITTED txn at commitTs(1500). New writer's snapshot is ts(1200).
- **Steps**: New txn write to same key.
- **Assert**: Write fails with `"Conflicting committed transaction"`.
- **Existing**: `SnapshotIsolationLostUpdatePreventionTest` (via natural clock skew), `LostUpdateSeparatedSnapshotsTest`, `LostUpdateClockPushTest`.

### 5.4 Intent's txn is ABORTED → delete intent, then write
- **Invariant**: Aborted intents do not block new writers.
- **Setup**: Pre-seed ABORTED txn record; intent at ts(1100). New txn at snapshot ts(1300) writes the key.
- **Steps**: Write.
- **Assert**: Old intent deleted; new intent written.
- **GAP**.

---

## 6. HLC propagation and lost-update prevention (the YB defining property)

These are the scenarios that demonstrate why HLC-based SI is correct without a global timestamp oracle. **The most important section.**

### 6.1 Single-node leading/lagging clock — lost update rejected
- **Invariant**: A leading-clock reader's stale write is rejected after a lagging-clock writer commits, because HLC propagation pushes the lagging commit's timestamp above the leading reader's snapshot.
- **Setup**: One participant node, two clients with different clock offsets.
- **Steps**: Leading client begins (high snapshot); lagging client begins; lagging writes intent at low TS; leading reads (sees pending intent → status RPC pushes coordinator's HLC); lagging commits (commitTs forced > leading snapshot); leading writes → rejected.
- **Assert**: `laggingCommitTs > leadingSnapshot`; leading write fails with `"Conflicting committed transaction"`.
- **Existing**: `SnapshotIsolationLostUpdatePreventionTest`.

### 6.2 Multi-node naturally separated snapshots — lost update rejected
- **Invariant**: A slow-clock client on an isolated coordinator gets a stale snapshot; first-committer-wins still rejects its later write.
- **Setup**: Three nodes; topology constraint that the stale reader's coordinator is on a node untouched by the fast committer's operations (so its HLC stays low).
- **Steps**: Fast client begin+write+commit; slow client begin (low snapshot) + read (sees nothing) + write → rejected.
- **Assert**: `slowSnapshot < fastSnapshot`; `fastCommitTs > slowSnapshot`; slow write fails.
- **Existing**: `LostUpdateSeparatedSnapshotsTest`.

### 6.3 Multi-node clock-push via status RPC
- **Invariant**: A fast-clock reader's status check pushes the slow-clock writer's coordinator HLC forward, forcing the writer's eventual commit timestamp above the reader's snapshot.
- **Setup**: Three nodes; topology constraint that key owner, writer's coordinator, reader's coordinator are all different nodes.
- **Steps**: Slow writer begin+write intent (low TS); fast reader begin (high snapshot) + read (sees intent → status RPC propagates high HLC to slow writer's coordinator); slow writer commit (now at forced-high TS); fast reader write → rejected.
- **Assert**: `slowCommitTs > fastSnapshot`; coordinator-of-slow's HLC > reader's pre-read HLC; fast write fails.
- **Existing**: `LostUpdateClockPushTest`.

### 6.4 Status RPC propagates HLC even when status is PENDING
- **Invariant**: HLC propagation is a property of the RPC itself, not of the response semantics. PENDING does not exempt the participant from clock-bumping the coordinator.
- **Setup**: Three nodes; same as 6.3 but observe pre-/post-RPC HLC.
- **Steps**: Pre-record coordinator HLC; reader triggers status RPC that returns PENDING; observe coordinator HLC.
- **Assert**: Coordinator HLC after RPC > before RPC; coordinator HLC ≥ reader's snapshot.
- **GAP** (currently entangled inside 6.3; worth a focused micro-test).

---

## 7. Snapshot Isolation semantics

### 7.1 Write skew across two keys is permitted
- **Invariant**: SI is *not* serializable — concurrent txns reading the same two keys and writing different keys both commit.
- **Setup**: Pre-seed `doctor-alice="on-call"`, `doctor-bob="on-call"` at ts(900).
- **Steps**: T1 reads both, writes `doctor-alice="off-call"`. T2 reads both, writes `doctor-bob="off-call"`. Both commit.
- **Assert**: Both commits succeed; both keys end up `"off-call"` (the invariant "at least one doctor must be on-call" is violated, demonstrating SI's anomaly).
- **Existing**: `SnapshotIsolationAnomalyTest`.

### 7.2 Read-only multi-key transaction sees a consistent snapshot
- **Invariant**: Across multiple reads in one txn, the observed values correspond to a single point in time (the snapshot).
- **Setup**: Pre-seed `keyA="v1a"` at ts(900), `keyB="v1b"` at ts(900). After T1 begins at snapshot ts(1000), a separate writer commits `keyA="v2a"`, `keyB="v2b"` at ts(1100).
- **Steps**: T1 reads `keyA`, then reads `keyB`.
- **Assert**: Both reads return the `ts(900)` values; T1 cannot observe a mix of pre- and post-writer state.
- **GAP**.

### 7.3 Read-your-own-write inside a multi-write transaction
- **Invariant**: Within a txn, a later read sees the latest written value, not the committed-below.
- **Setup**: Pre-seed `key="v1"` at ts(900).
- **Steps**: T1 begin, write `"v2"`, read `key`, write `"v3"`, read `key`.
- **Assert**: First read returns `"v2"`, second read returns `"v3"`.
- **GAP**.

---

## 8. Clock uncertainty and read restart

The most consequential within-scope correctness gap in the current repo.

### 8.1 Snapshot read can miss a committed value inside the uncertainty window (the problem)
- **Invariant**: Without read-restart, SI is unsafe under bounded clock skew.
- **Setup**: Storage node clock at 1005; writer client at 995; reader client at 1000. `max_clock_skew = 10`. Writer commits a value; reader begins at snapshot 1000 and reads at an explicit snapshot.
- **Steps**: Writer begin+write+commit; reader begin; reader read.
- **Assert**: The committed value's HLC is in (1000, 1010] — i.e., inside the reader's uncertainty window — and the read returns no value. Reading at snapshot > 1010 returns the value.
- **Existing**: `ClockUncertaintySnapshotTest` (demonstrates the *gap*, not the fix).

### 8.2 Read-restart: reader detects in-uncertainty commit and restarts at higher snapshot
- **Invariant**: When the read path encounters a committed version with HLC in `[snapshot, snapshot + max_clock_skew]`, the participant returns `ReadRestartRequired` and the client retries at a snapshot ≥ that version's HLC.
- **Setup**: Same as 8.1.
- **Steps**: Reader read at snapshot 1000.
- **Assert**: First read returns `ReadRestartRequired` with `restartTs ≥ commitTs`; client retries at `restartTs`; second read returns the committed value.
- **GAP** (module `05-clock-uncertainty-and-read-restart` is named for this).

### 8.3 Read-restart respects the uncertainty bound — does not restart past it
- **Invariant**: A version with HLC > `snapshot + max_clock_skew` does **not** trigger a restart (it's simply invisible).
- **Setup**: Skew = 10; commit at ts(1020); reader at snapshot 1000.
- **Steps**: Reader read.
- **Assert**: Read returns the committed-below state with no restart.
- **GAP**.

---

## 9. Multi-participant atomicity

### 9.1 Multi-key txn writes intents across multiple participant tablets
- **Invariant**: Each touched key's owner receives an intent for the txn.
- **Setup**: Three nodes; routing such that `keyA → node1`, `keyB → node2`, `keyC → node3`.
- **Steps**: Begin, write all three keys.
- **Assert**: Each owner has an intent for the txn; coordinator's `participantReplicas` (after commit) contains all three.
- **GAP**.

### 9.2 Commit triggers resolve RPCs to every participant; all-or-nothing visibility
- **Invariant**: After commit, every participant eventually has the committed value visible at `commitTs`.
- **Setup**: As 9.1.
- **Steps**: Commit; tick cluster until all participants converge.
- **Assert**: For every key, `committedStore.getAsOf(key, commitTs) == value`; all intents cleared.
- **GAP**.

### 9.3 Snapshot read at `commitTs - 1` observes none of the multi-key commit; at `commitTs` observes all
- **Invariant**: Multi-participant commits are atomically visible at the commit timestamp.
- **Setup**: As 9.1. After commit, second txn at snapshot `commitTs - 1`, then a third at snapshot `commitTs`.
- **Steps**: Both txns read all three keys.
- **Assert**: First txn observes none of the new values; third observes all three.
- **GAP**.

### 9.4 Lost resolve RPC to one participant is recovered by the next reader/writer
- **Invariant**: An unresolved intent on a participant after commit does not corrupt SI; the next read or write on that key resolves it via status check.
- **Setup**: Drop `ResolveTransactionRequest` between coordinator and one specific participant.
- **Steps**: Commit succeeds. Then a second client writes the same key.
- **Assert**: Coordinator status is COMMITTED; participant intent persists until the second client's write triggers resolution; new value is then visible.
- **Existing**: `TransactionalStorageReplicaWriteResolutionTest.txnWriteResolvesLingeringCommittedIntentAfterDroppedResolveRequest`.

### 9.5 Mid-commit resolve dropped to *some* participants → readers on those participants drive resolution
- **Invariant**: Same as 9.4 but exercised by the read path.
- **Setup**: As 9.4, but the second client reads instead of writes.
- **Steps**: Commit; drop resolve to participant X; second client reads the key on X at high snapshot.
- **Assert**: Read materializes the intent and returns the committed value.
- **GAP**.

---

## 10. Liveness: heartbeat-based abort

YugabyteDB drives this via client → status tablet heartbeats. This repo inverts the direction (coordinator-side timeout). The scenarios below are framed for whichever direction the implementation chooses.

### 10.1 Abandoned PENDING txn is reclaimed after heartbeat timeout
- **Invariant**: A txn whose client stops heartbeating is eventually aborted; its coordinator record is removed.
- **Setup**: Begin a txn; do not commit. Tick the cluster past the heartbeat timeout.
- **Steps**: Tick.
- **Assert**: Coordinator's `txnRecords` no longer contains the txn (or contains it with status ABORTED, depending on retention policy).
- **GAP**.

### 10.2 Reader resolves a leftover intent from a timed-out txn as ABORTED
- **Invariant**: A pending intent whose owning txn record is gone is treated as aborted, not as eternally pending.
- **Setup**: Same as 10.1; intent was written before timeout.
- **Steps**: After timeout, a second client at high snapshot reads the key.
- **Assert**: Intent is deleted; read returns committed-below.
- **GAP**.

### 10.3 Writer succeeds on the same key after a previous txn's heartbeat timeout
- **Invariant**: Cleanup of timed-out txns unblocks new writers.
- **Setup**: As 10.1.
- **Steps**: After timeout, second client writes the same key.
- **Assert**: Write succeeds; new intent at new provisional HLC.
- **GAP**.

---

## 11. Core invariants (cross-cutting assertions)

These are not standalone tests, but should be asserted across many of the above scenarios.

- **I-1 Snapshot ≥ coordinator HLC at begin**: A txn's snapshot is at least the coordinator's HLC at begin time.
- **I-2 Commit timestamp ≥ all intent timestamps**: For every intent written by a txn, `commitTs ≥ intentTs`.
- **I-3 Commit timestamp ≥ HLC of all coordinators contacted**: HLC propagation forces this.
- **I-4 First-committer-wins is symmetric**: For any two txns T1, T2 writing the same key with overlapping snapshots, exactly one commits successfully; the other's write is rejected.
- **I-5 No torn visibility**: For any key set written by a single txn, no reader at any snapshot observes a strict subset of those writes.
- **I-6 Read-monotonicity within a txn**: A txn re-reading the same key in absence of its own writes always returns the same value.
- **I-7 Read uncertainty bound**: For any snapshot S and skew K, a successful (non-restarted) read observes all commits with HLC ≤ S and no commits with HLC > S + K. (Requires read-restart from §8.)

---

## Coverage summary

| Section | Existing | Gap |
|---|---|---|
| 1. HLC mechanics | — (module 01) | 1.1–1.3 if not already there |
| 2. MVCC storage | 2.1, 2.2 | 2.3, 2.4 |
| 3. Single-txn lifecycle | 3.1–3.3 | 3.4 |
| 4. Read-path resolution | 4.1, 4.2 | 4.3, 4.4 |
| 5. Write-path resolution | 5.1, 5.2, 5.3 | 5.4 |
| 6. HLC propagation / lost-update | 6.1, 6.2, 6.3 | 6.4 |
| 7. SI semantics | 7.1 | 7.2, 7.3 |
| 8. Clock uncertainty / read restart | 8.1 (problem only) | 8.2, 8.3 |
| 9. Multi-participant atomicity | 9.4 | 9.1, 9.2, 9.3, 9.5 |
| 10. Heartbeat-based abort | — | 10.1, 10.2, 10.3 |
| 11. Invariants | partial | most |

**Highest-value gaps to close next:**
1. §8.2 read-restart — closes the only latent SI correctness gap inside the stated scope.
2. §9.1–9.3 multi-participant atomicity — exercises the protocol's distinguishing feature against single-key tests.
3. §10.1–10.3 heartbeat/abort — completes the liveness story.
4. §4.3, 4.4, 5.4 — fills the remaining intent-resolution status cells (currently 5/8 of the 4×2 matrix is covered).
