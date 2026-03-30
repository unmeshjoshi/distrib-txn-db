# Isolation Levels

This table compares the user-visible anomalies prevented by each isolation level and the typical
way reads are protected in common implementations.

The first five columns describe semantics. The last column describes implementation style, which
can vary across databases. In particular, MVCC systems often avoid blocking read locks even when a
locking-based database would use them.

| Isolation Level | Dirty Read (Reading uncommitted changes) | Non-Repeatable Read (Re-reading a row gives different data) | Phantom Read (Range queries find new/missing rows) | Lost Update (Concurrent updates overwrite each other) | Write Skew (Updating different rows breaks a rule) | Typical Read Coordination |
| --- | --- | --- | --- | --- | --- | --- |
| Read Uncommitted | Possible | Possible | Possible | Possible | Possible | No Read Locks |
| Read Committed | No | Possible | Possible | Possible | Possible | Usually statement-level visibility. Locking engines may use short-lived shared locks; MVCC engines often use committed versions without holding read locks. |
| Repeatable Read | No | No | Possible | No* | Possible | Engine-dependent. Locking engines may hold shared read locks until commit. MVCC engines often keep a stable row snapshot without blocking readers. |
| Snapshot Isolation | No | No | No** | No | Possible | Snapshot/MVCC read at a fixed timestamp. Reads typically do not take blocking read locks. |
| Serializable (Pessimistic) | No | No | No | No | No | Reads are usually protected with shared locks plus predicate or range locks so writes that would break serial order are blocked. |
| Serializable (SSI) | No | No | No | No | No | Reads usually do not block writers immediately, but the system tracks read dependencies using non-blocking read markers, predicate tracking, or similar structures. |

## Notes

- `No*`: lost update under `Repeatable Read` depends on the database engine and implementation.
  Some engines prevent it directly. Others still need explicit locking or stronger conflict
  detection.
- `No**`: snapshot isolation gives a stable snapshot for the transaction, so re-reading the same
  rows from that snapshot does not change and range queries are evaluated against that same
  snapshot. Even so, snapshot isolation is not fully serializable and can still allow anomalies
  like write skew.

## How To Read The Last Column

- `No Read Locks` means the database does not normally block reads using classic shared locks.
  This is common in MVCC-based systems.
- `Shared locks` means other transactions can often still read, but conflicting writes may be
  blocked until the transaction finishes.
- `Predicate` or `range` locking means the database protects not only existing rows that were read,
  but also the key range or search condition, which is how pessimistic serializable systems prevent
  phantom rows from appearing.
- `Non-blocking read markers` means the database remembers what was read without necessarily
  blocking immediately. Serializable Snapshot Isolation (SSI) then aborts one transaction later if
  those recorded read/write dependencies form a dangerous pattern.
