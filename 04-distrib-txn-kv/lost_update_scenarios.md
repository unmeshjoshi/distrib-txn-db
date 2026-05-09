# Lost Update Prevention with Snapshot Isolation and Hybrid Logical Clocks

## The Lost Update Anomaly

A "lost update" occurs when two transactions read the same value, each compute a new value from it, and the second to commit silently overwrites the first — the first update is "lost."

**Example:** An account has a balance of $1000. Two tellers each read the balance, each independently adds a $100 deposit, and each writes $1100. The account should be $1200, but the second commit overwrites the first. One deposit vanishes.

This anomaly is possible under Read Committed isolation (where each statement sees the latest committed data) but must be prevented under Snapshot Isolation.

## Snapshot Isolation's Defense: First-Committer-Wins

Under Snapshot Isolation, each transaction reads from a consistent snapshot defined by its **read timestamp** (readTs). All reads see the database as it existed at that point — later commits are invisible.

The defense against lost updates is the **first-committer-wins rule**: when a transaction attempts to write a key, the system checks whether any other transaction has already committed a newer version of that key after this transaction's readTs. If so, the write is rejected — the transaction that committed first wins, and the stale writer must abort and retry.

In a distributed system with multiple nodes and skewed clocks, Hybrid Logical Clocks (HLC) provide the timestamp ordering that makes this rule enforceable across the cluster.

---

## Scenario 1: Clock Skew Creates Naturally Separated Snapshots

This scenario illustrates the straightforward case: a fast-clock client commits first, and a slow-clock client's later write is rejected because it read from a stale snapshot.

### Setup
- **Cluster:** Three storage nodes: `ATHENS`, `BYZANTIUM`, and `CYRENE`. All clocks start at `(1000, 0)`.
- **Clients:**
    - **Alice (fast clock):** Wall clock is `2000` (skewed ahead).
    - **Bob (normal clock):** Wall clock is `1000`.
- **Routing assumptions:** Key `"Account1"` is owned by `ATHENS`. Alice's transaction coordinator is `ATHENS`. Bob's transaction coordinator is `CYRENE`.

### Sequence of Events

**Step 1: Alice begins her transaction**
- Alice sends `beginTransaction` to her coordinator (`ATHENS`).
- `ATHENS` receives the request carrying Alice's client time `2000`. It merges this into its HLC, producing `(2000, 1)`.
- Alice's **readTs = (2000, 1)**.

**Step 2: Alice writes a provisional intent**
- Alice writes `"Account1"` on `ATHENS` (the key's owner).
- `ATHENS` ticks its HLC and stores the intent at `(2000, 2)`.

**Step 3: Bob begins his transaction**
- Bob sends `beginTransaction` to `CYRENE` (his coordinator).
- `CYRENE`'s clock has not been touched by Alice's requests — it is still at `(1000, 0)`. It merges Bob's client time `1000`, producing `(1000, 1)`.
- Bob's **readTs = (1000, 1)**.

**Step 4: Bob reads the key**
- Bob sends a read for `"Account1"` to `ATHENS`.
- `ATHENS` sees Alice's provisional intent at `(2000, 2)`.
- Bob's readTs `(1000, 1)` is strictly less than the intent's timestamp `(2000, 2)`, so the intent is in his "future" — invisible to his snapshot.
- Bob reads the older committed value (written at some time < 1000).

**Step 5: Alice commits**
- Alice sends a commit request to `ATHENS`.
- `ATHENS` assigns a **commitTs = (2000, 3)**.
- Alice's intent is finalized and moved to the committed store at `(2000, 3)`.

**Step 6: Bob attempts to write (REJECTED)**
- Based on the old value he read in Step 4, Bob computes a new value and attempts to write `"Account1"`.
- `ATHENS` performs **first-committer-wins validation**: is there any committed version of `"Account1"` with a timestamp strictly greater than Bob's readTs `(1000, 1)`?
- Yes — Alice's committed version at `(2000, 3)`.
- Because `(2000, 3) > (1000, 1)`, Bob's write is **rejected**.

### Why it works
The large clock skew makes the ordering obvious: Bob's snapshot is clearly behind Alice's commit. The HLC timestamps make this comparison mechanical — no global coordinator or synchronization is needed.

---

## Scenario 2: HLC Propagation Pushes a Lagging Commit Forward

This is the subtler case. The writer starts with a *lower* clock than the reader, so naively its commit timestamp might end up below the reader's snapshot — which would allow a lost update. HLC propagation during the read prevents this by pushing the writer's coordinator clock forward.

### Setup
- **Cluster:** Three storage nodes: `ATHENS`, `BYZANTIUM`, and `CYRENE`. All clocks start at `(1000, 0)`.
- **Clients:**
    - **Alice (fast clock):** Wall clock is `2000`.
    - **Bob (normal clock):** Wall clock is `1000`.
- **Routing assumptions:** Key `"Account1"` is owned by `BYZANTIUM`. Alice's transaction coordinator is `ATHENS`. Bob's transaction coordinator is `CYRENE`.

### Sequence of Events

**Step 1: Bob begins his transaction (lagging clock)**
- Bob sends `beginTransaction` to `CYRENE`.
- `CYRENE` merges Bob's client time `1000`, producing `(1000, 1)`.
- Bob's **readTs = (1000, 1)**.

**Step 2: Alice begins her transaction (leading clock)**
- Alice sends `beginTransaction` to `ATHENS`.
- `ATHENS` merges Alice's client time `2000`, producing `(2000, 1)`.
- Alice's **readTs = (2000, 1)**.

**Step 3: Bob writes a provisional intent**
- Bob writes `"Account1"` to `BYZANTIUM`.
- `BYZANTIUM` merges Bob's client time. Its clock becomes `(1000, 2)`.
- The provisional intent is stored at `(1000, 2)`.

**Step 4: Alice reads the key (the crucial clock push)**
- Alice sends a read for `"Account1"` to `BYZANTIUM`. Her request carries her high clock value `(2000, 1)`.
- `BYZANTIUM` merges this, pushing its clock to `(2000, 2)`.
- Alice sees Bob's pending intent at `(1000, 2)`. Since `(1000, 2)` < Alice's readTs `(2000, 1)`, she must determine whether this intent is from a committed or still-pending transaction.
- `BYZANTIUM` sends a `GetTransactionStatus` RPC to Bob's coordinator (`CYRENE`). This RPC carries the current HLC: `(2000, 2)`.
- **The clock push:** When `CYRENE` receives this RPC, it merges `(2000, 2)` into its own clock. `CYRENE` jumps from `(1000, 1)` to `(2000, 3)`.
- `CYRENE` responds that Bob's transaction is `PENDING`. Alice ignores the intent and reads the older committed value.

**Step 5: Bob commits (forced to a high timestamp)**
- Bob sends `commitTransaction` to `CYRENE`.
- `CYRENE` assigns the commit timestamp from its *current* clock — which was pushed forward in Step 4.
- Bob's **commitTs = (2000, 4)**.
- Bob's intent at `BYZANTIUM` is resolved to the committed store at `(2000, 4)`.

**Step 6: Alice attempts to write (REJECTED)**
- Alice computes a new value from her stale read and tries to write `"Account1"` to `BYZANTIUM`.
- `BYZANTIUM` performs **first-committer-wins validation**: is there a committed version after Alice's readTs `(2000, 1)`?
- Yes — Bob's committed version at `(2000, 4)`.
- Because `(2000, 4) > (2000, 1)`, Alice's write is **rejected**.

### Why it works
Without the clock push in Step 4, Bob's coordinator might have committed at a low timestamp like `(1000, 2)` — below Alice's readTs of `(2000, 1)`. Alice's write would then pass the first-committer-wins check, overwriting Bob's update (a lost update).

The HLC propagation during Alice's read ensures that Bob's coordinator *must* commit at a timestamp above Alice's snapshot. The read itself acts as a "warning" — it informs the cluster that someone is observing from a high timestamp, so any pending transaction that might conflict must commit above that observation point.

---

## Summary

| Scenario | Key Insight |
|----------|-------------|
| 1. Naturally separated snapshots | When the committer's clock is already ahead of the stale reader's snapshot, the first-committer-wins check trivially rejects the stale write. |
| 2. Clock push via HLC propagation | When the committer's clock is *behind* the reader's snapshot, the read's HLC propagation pushes the committer's coordinator forward, ensuring the commit timestamp ends up above the reader's snapshot. |

Together, these two scenarios demonstrate that HLC propagation makes the first-committer-wins rule work regardless of clock skew direction — the system prevents lost updates whether the first committer has a faster or slower clock than the stale reader.
