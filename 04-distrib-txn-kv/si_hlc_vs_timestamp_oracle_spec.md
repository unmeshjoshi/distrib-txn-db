# Snapshot Isolation with Timestamp Oracle vs Hybrid Logical Clocks

_Spec note with examples of both approaches_

## Overview

This note describes two ways to assign timestamps for Snapshot Isolation:

1. a **Timestamp Oracle (TO)**
2. **Hybrid Logical Clocks (HLC)**

Both can implement Snapshot Isolation, but they rely on different timestamp assumptions.

A timestamp oracle assumes a **global monotonic order of timestamps** across the system. An HLC-based
design uses a weaker timestamp discipline, so it must add an extra safety mechanism:
**read restart / uncertainty handling**. In this workshop, `04` introduces snapshot isolation over
HLC-ordered versions, and `05` adds read restart as the missing requirement that makes the chosen
snapshot timestamp safe under clock skew.

---

## 1. Common Snapshot Isolation Model

We consider the same transactional model for both approaches.

```text
Nodes := {N1, N2}
Keys  := {x}
Transactions := {T1, T2}
```

Each committed version of key `x` is represented as:

```text
Version(x) := <value, commit_ts, writer_txn>
```

Each transaction `T` maintains:

```text
coord(T)    : coordinator node
R(T)        : snapshot timestamp
C(T)        : commit timestamp
writeSet(T) : set of keys written by T
state(T)    : {active, committed, aborted}
```

Each read uses the SI visibility rule:

```text
Read(T, x):
    return latest committed visible version v of x
           such that v.commit_ts <= R(T)
```

Each commit uses first-committer-wins validation:

```text
Validate(T):
    for each key k in writeSet(T):
        let latest := LatestCommittedVersion(k)
        if latest.commit_ts > R(T):
            return abort
    return commit
```

Each successful commit installs all writes atomically at `C(T)`:

```text
Commit(T):
    if Validate(T) = abort:
        state(T) := aborted
    else:
        choose commit timestamp C(T)
        install all writes of T at C(T)
        state(T) := committed
```

---

## 2. Specification A: Snapshot Isolation with Timestamp Oracle

A timestamp oracle assigns globally monotonic timestamps, giving a single global timestamp order for all transactions.

### Rules

#### Begin

```text
Begin(T, N):
    coord(T) := N
    R(T)     := TO.next()
    writeSet(T) := {}
    state(T) := active
```

#### Blind Write

```text
BlindWrite(T, x, value):
    writeSet(T) := writeSet(T) union {x}
```

#### Commit

```text
Commit(T):
    if Validate(T) = abort:
        state(T) := aborted
    else:
        C(T) := TO.next()
        install all writes of T at C(T)
        state(T) := committed
```

### Timestamp Oracle Property

```text
Global Monotonicity:
    every new timestamp is greater than every previously assigned timestamp
```

### Example A: Blind Writes with Timestamp Oracle (Ordered Case)

#### Initial State

```text
x = v0 @ 0
```

#### Execution

```text
1. Begin(T1, N1)
       R(T1) := 1

2. BlindWrite(T1, x, v1)

3. Commit(T1)
       latest version of x is v0 @ 0
       0 <= 1, so validation succeeds
       C(T1) := 2
       install x = v1 @ 2

4. Begin(T2, N2)
       R(T2) := 3

5. BlindWrite(T2, x, v2)

6. Commit(T2)
       latest version of x is v1 @ 2
       2 <= 3, so validation succeeds
       C(T2) := 4
       install x = v2 @ 4
```

#### Interpretation

So `T1` is in the visible past of `T2`:

```text
C(T1) = 2 < R(T2) = 3
```

A timestamp oracle often imposes a stronger assumption than SI strictly needs: blind writes are naturally placed into one global order, whereas SI fundamentally requires exclusion only of same-key write-write conflicts among transactions that are concurrent relative to their snapshots.

### Example A2: Blind Writes with Timestamp Oracle (Conflict Case)

#### Initial State

```text
x = v0 @ 0
```

#### Execution

```text
1. Begin(T1, N1)
       R(T1) := 1

2. BlindWrite(T1, x, v1)

3. Begin(T2, N2)
       R(T2) := 2

4. Commit(T1)
       latest version of x is v0 @ 0
       0 <= 1, so validation succeeds
       C(T1) := 3
       install x = v1 @ 3

5. BlindWrite(T2, x, v2)

6. Commit(T2)
       latest version of x is v1 @ 3
       since 3 > R(T2)=2,
       validation fails
       T2 aborts
```

#### Interpretation

Here `T2` begins after `T1` but before `T1` commits. Because `T1`’s write becomes visible only at timestamp `3`, which is after `T2`’s snapshot timestamp `2`, the two blind writes are concurrent relative to `T2`’s snapshot. Therefore `T2` detects a same-key write-write conflict and aborts.

---

## 3. Specification B: Snapshot Isolation with Hybrid Logical Clocks

Each node `N` maintains a physical clock `PT(N)` and a hybrid logical clock `HLC(N) = <pt, lc>`. In
this simplified model, the transaction snapshot timestamp is the coordinator’s local HLC at begin
time. A production HLC-based SI system needs more than this simplified begin rule: it also needs
read restart or a similar safe-snapshot mechanism so reads do not trust a snapshot that may be
ambiguous under clock skew.

```text
R(T) := HLC(coord(T)) at begin time
```

Timestamps are globally comparable lexicographically:

```text
<p1, l1> < <p2, l2>
    iff p1 < p2
    or (p1 = p2 and l1 < l2)
```

### Rules

#### Begin

```text
Begin(T, N):
    coord(T) := N
    R(T)     := HLC(N)
    writeSet(T) := {}
    state(T) := active
```

#### HLC Update

```text
LocalEvent(N):
    HLC(N) := AdvanceLocal(HLC(N), PT(N))

ReceiveEvent(N, ts):
    HLC(N) := Merge(HLC(N), PT(N), ts)
```

#### Blind Write

```text
BlindWrite(T, x, value):
    writeSet(T) := writeSet(T) union {x}
```

#### Commit

```text
Commit(T):
    if Validate(T) = abort:
        state(T) := aborted
    else:
        C(T) := FreshTS(max(R(T), HLC(coord(T))))
        install all writes of T at C(T)
        state(T) := committed
```

### HLC Property

```text
Write-Follows-Read:
    if transaction T reads from node N while executing with snapshot R(T),
    then after N incorporates R(T) through HLC update,
    no causally subsequent commit on N may be assigned a timestamp < R(T)
```

Transaction-level consequence:

```text
if commit of T depends on that read, then C(T) >= R(T)
```

This is weaker than global monotonicity. It is useful for preserving causality, but by itself it is
not the full correctness story for HLC-based SI. The missing piece is safe snapshot selection,
which in this workshop is provided by read restart in module `05`.

### Example B: Blind Writes with Clock Skew and HLC

#### Initial State

```text
PT(N1)  = 3
PT(N2)  = 5
HLC(N1) = <3,0>
HLC(N2) = <5,0>

x = v0 @ <0,0>
```

#### Execution

```text
1. Begin(T1, N1)
       R(T1) := <3,0>

2. BlindWrite(T1, x, v1)

3. Commit(T1)
       latest version of x is v0 @ <0,0>
       <0,0> <= <3,0>, so validation succeeds
       choose C(T1) = <4,0>
       install x = v1 @ <4,0>

4. Begin(T2, N2)
       R(T2) := <5,0>

5. BlindWrite(T2, x, v2)

6. Commit(T2)
       latest version of x is v1 @ <4,0>
       since <4,0> <= <5,0>, validation succeeds
       choose C(T2) = <5,1> or <6,0>
       install x = v2 @ C(T2)
```

#### Interpretation

So `T1` is not concurrent with `T2` in SI timestamp order:

```text
C(T1) <= R(T2)
```

The skew between `N1` and `N2` does not violate SI in this execution, because `C(T1) <= R(T2)`.

### Example C: Read Followed by Write in HLC

#### Initial State

```text
PT(N1)  = 3
PT(N2)  = 5
HLC(N1) = <3,0>
HLC(N2) = <5,0>
x = v0 @ <0,0>
```

#### Execution

```text
1. Begin(T2, N2)
       R(T2) := <5,0>

2. T2 reads x from N1
       visible version is x = v0 @ <0,0>

3. N1 incorporates R(T2)=<5,0> through HLC update

4. T1 begins on N1 and blind-writes x

5. Commit(T1)
       because N1 has incorporated <5,0>,
       no causally downstream commit on N1 may be assigned timestamp < <5,0>
       choose C(T1) = <5,1>
       install x = v1 @ <5,1>

6. T2 now blind-writes x

7. Commit(T2)
       latest version of x is v1 @ <5,1>
       since <5,1> > R(T2)=<5,0>,
       validation fails
       T2 aborts
```

#### Interpretation

This example shows both the HLC write-follows-read mechanism and the SI conflict check: after `T2` reads from `N1` with snapshot `R(T2)=<5,0>`, `N1` cannot assign a causally downstream commit timestamp below `<5,0>`. Therefore `T1`’s conflicting commit on `x` occurs at `<5,1>`, and when `T2` later tries to write `x`, SI validation detects `LatestCommittedVersion(x).ts > R(T2)` and aborts `T2`.

---

## 4. Comparison and Correctness Claim

A timestamp oracle gives Snapshot Isolation by imposing a global monotonic timestamp order. This
makes snapshot selection and conflict handling simple.

HLC can provide a weaker discipline through write-follows-read. However, HLC itself does not detect
write-write conflicts, and HLC alone does not make a chosen snapshot timestamp safe. Conflict
detection still comes from SI validation against the transaction snapshot, and the snapshot itself
must be protected by read restart / uncertainty handling.

### Timestamp Oracle SI

Correct and simple because:

- `R(T)` and `C(T)` come from one globally monotonic source
- reads use latest committed version `<= R(T)`
- same-key conflicting writes after `R(T)` are rejected by validation
- committed writes appear atomically at `C(T)`

### HLC SI

Sufficient in this model when:

- `R(T)` is the coordinator HLC at begin
- reads use read restart / uncertainty handling so `R(T)` is safe under clock skew
- reads use latest committed version `<= R(T)`
- HLC enforces write-follows-read for causally downstream work
- same-key conflicting writes after `R(T)` are rejected by validation
- committed writes appear atomically at `C(T)`

---

## Final Summary

- **Timestamp oracle SI**: correct and simple, but globally monotonic by construction.
- **HLC SI**: sufficient when snapshot visibility, read restart / uncertainty handling, write-follows-read, first-committer-wins validation, and atomic visibility are all enforced.

HLC is naturally strong for read-modify-write causality; blind-write correctness still depends on the validation rule and on how snapshot timestamps are chosen.
