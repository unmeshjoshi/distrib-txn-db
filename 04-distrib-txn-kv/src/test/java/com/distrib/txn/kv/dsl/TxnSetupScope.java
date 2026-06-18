package com.distrib.txn.kv.dsl;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.testkit.dsl.SetupScope;
import com.tickloom.testkit.dsl.semanticmodel.ClusterEvent;

/**
 * Txn-specific initial-condition verbs. Different protocols define their
 * own scope (Quorum has timestamped storage seeds, Raft would have log /
 * term setup, etc.) — there is no shared notion of "clock" or "seed" in
 * the base {@link SetupScope}.
 */
public interface TxnSetupScope extends SetupScope {
    /**
     * Seeds a client's {@code HybridClock} to the given timestamp and
     * aligns the simulated wall clock for that process.
     */
    TxnSetupScope clientHlc(ProcessId clientId, HybridTimestamp timestamp);

    /**
     * Seeds a single node's {@code HybridClock} to the given timestamp and
     * aligns the simulated wall clock for that process.
     *
     * <p>Lets a scenario model per-node clock skew (e.g. a lagging coordinator
     * vs a leading one) tied to the routing role a node plays. This matters
     * because a client's snapshot is the {@code propagatedTime} its coordinator
     * returns from {@code beginTransaction} — i.e. {@code max(coordinator node
     * HLC, client HLC)} — not the client's raw HLC. To put the lead/lag where
     * the scenario needs it, the node clocks must be seeded, not just the
     * client clocks.
     */
    TxnSetupScope nodeHlc(ProcessId nodeId, HybridTimestamp timestamp);

    /**
     * Seeds a client's {@code HybridClock} to the given timestamp and
     * aligns the simulated wall clock for that process.
     */
    TxnSetupScope hlcForAllNodes(HybridTimestamp timestamp);


    @Override
    TxnSetupScope apply(ClusterEvent event);
}
