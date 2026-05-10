package com.distrib.txn.kv.dsl;

import org.junit.jupiter.api.Test;

import static com.distrib.txn.kv.dsl.TopologyConstraint.*;
import static com.distrib.txn.kv.dsl.TransactionScenario.hlc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lost Update Scenario 1: Naturally Separated Snapshots.
 *
 * Alice (fast clock) writes and commits. Bob (slow clock, isolated coordinator)
 * reads a stale snapshot then tries to overwrite — rejected by first-committer-wins.
 */
class LostUpdateSeparatedSnapshotsDslTest {

    @Test
    void fastCommitterWins_staleReaderWriteRejected() {
        ScenarioResult result = TransactionScenario.create()
                .nodes(3)
                .client("alice", hlc(2000))
                .client("bob", hlc(1000))
                .topology(where("bob").coordinatorIsNotSameAs(keyOwner())
                                      .coordinatorIsNotSameAs(coordinatorOf("alice")))
                .begin("alice")
                .write("alice", "Account1", "100").expectSuccess()
                .begin("bob")
                .read("bob", "Account1").expectNotFound()
                .commit("alice")
                .write("bob", "Account1", "200").expectRejected("Conflicting committed transaction")
                .run();

        assertTrue(result.commitTsOf("alice").compareTo(result.snapshotOf("bob")) > 0,
                "Alice's commit must be above bob's snapshot");
        assertTrue(result.hlcAt(coordinatorOf("bob")).compareTo(hlc(1500)) < 0,
                "Bob's coordinator was never pushed — its HLC stayed low");
        assertEquals("100", result.committedValue("Account1"));
    }
}
