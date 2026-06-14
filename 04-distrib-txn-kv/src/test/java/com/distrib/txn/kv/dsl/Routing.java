package com.distrib.txn.kv.dsl;

import com.distrib.txn.kv.ReplicaRouting;
import com.distrib.txn.kv.TxnId;
import com.tickloom.ProcessId;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * Finds a {@code (key, txnId1, txnId2)} triple whose hash routing satisfies a
 * test-specific topology constraint.
 *
 * <p>Many snapshot-isolation scenarios in this repo depend on specific role
 * separations between cluster nodes — e.g. "the slow client's coordinator must
 * be a different node from the key owner so HLC merges from the writer's
 * intent don't drag the slow client's snapshot forward." Hash-mod-N routing
 * makes the node each role lands on a deterministic function of the chosen
 * key and txnIds, so a test cannot pick role-distinct nodes directly — it has
 * to pick IDs whose routing happens to produce the layout it needs.
 *
 * <p>{@link #find(List, Predicate)} brute-forces over a pool of candidate keys
 * and txnIds, computes the resulting {@code (keyOwner, coord1, coord2)} triple
 * for each, and returns the first one whose computed nodes satisfy the
 * predicate. The predicate stays at the call site so the constraint reads as
 * documentation of what that specific scenario requires:
 *
 * <pre>{@code
 * // §6.2 — slow client's coord must be off the key owner and off the fast coord.
 * Routing r = Routing.find(servers,
 *         m -> !m.coord2().equals(m.keyOwner())
 *           && !m.coord2().equals(m.coord1()));
 * }</pre>
 *
 * <p>Candidate IDs ({@code "key-0".."key-19"}, {@code "txn-0".."txn-19"})
 * carry no semantic meaning — they are only the search pool the brute force
 * walks through looking for a hash combination that lands the right way.
 */
public record Routing(
        String key,
        TxnId txnId1,
        TxnId txnId2,
        ProcessId keyOwner,
        ProcessId coord1,
        ProcessId coord2
) {

    private static final int CANDIDATE_POOL_SIZE = 20;

    private static final List<String> CANDIDATE_KEYS = IntStream.range(0, CANDIDATE_POOL_SIZE)
            .mapToObj(i -> "key-" + i)
            .toList();

    private static final List<TxnId> CANDIDATE_TXNS = IntStream.range(0, CANDIDATE_POOL_SIZE)
            .mapToObj(i -> TxnId.of("txn-" + i))
            .toList();

    /**
     * Brute-force search for an ID combination whose routing satisfies the constraint.
     *
     * @throws AssertionError if no combination in the candidate pool satisfies
     *         the constraint — usually means the constraint is geometrically
     *         unsatisfiable on the given cluster (e.g. "all roles distinct" on
     *         a 2-node cluster) or the candidate pool is too small.
     */
    public static Routing find(List<ProcessId> servers, Predicate<Routing> constraint) {
        List<ProcessId> canonical = ReplicaRouting.canonicalReplicaOrder(servers);
        for (String key : CANDIDATE_KEYS) {
            ProcessId keyOwner = ReplicaRouting.replicaFor(key, canonical);
            for (TxnId t1 : CANDIDATE_TXNS) {
                ProcessId coord1 = ReplicaRouting.coordinatorFor(t1, canonical);
                for (TxnId t2 : CANDIDATE_TXNS) {
                    if (t2.equals(t1)) {
                        continue;
                    }
                    Routing candidate = new Routing(
                            key, t1, t2,
                            keyOwner, coord1,
                            ReplicaRouting.coordinatorFor(t2, canonical));
                    if (constraint.test(candidate)) {
                        return candidate;
                    }
                }
            }
        }
        throw new AssertionError(
                "No routing satisfied the constraint over " + CANDIDATE_POOL_SIZE
                        + " candidate keys/txnIds on a " + servers.size() + "-node cluster");
    }
}
