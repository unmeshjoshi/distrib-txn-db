package com.distrib.txn.kv.dsl2;

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
     * Seeds a client's {@code HybridClock} to the given timestamp and
     * aligns the simulated wall clock for that process.
     */
    TxnSetupScope hlcForAllNodes(HybridTimestamp timestamp);


    @Override
    TxnSetupScope apply(ClusterEvent event);
}
