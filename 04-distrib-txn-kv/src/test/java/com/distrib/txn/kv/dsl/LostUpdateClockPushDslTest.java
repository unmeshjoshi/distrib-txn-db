package com.distrib.txn.kv.dsl;

import org.junit.jupiter.api.Test;

import static com.distrib.txn.kv.dsl.TopologyConstraint.*;
import static com.distrib.txn.kv.dsl.TransactionScenario.hlc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lost Update Scenario 2: HLC Propagation Pushes a Lagging Commit Forward.
 *
 * Bob (slow clock) writes first. Alice (fast clock) reads, pushing Bob's coordinator
 * clock forward via the status check RPC. Bob commits at a forced-high timestamp.
 * Alice's write is rejected by first-committer-wins.
 */
class LostUpdateClockPushDslTest {

    @Test
    void hlcPropagationPushesLaggingCommitAboveLeadingSnapshot() {
        ScenarioResult result = TransactionScenario.create()
                .nodes(3)
                .client("bob", hlc(1000))
                .client("alice", hlc(2000))
                .topology(where("bob").coordinatorIsNotSameAs(keyOwner())
                                      .coordinatorIsNotSameAs(coordinatorOf("alice"))
                          .and("alice").coordinatorIsNotSameAs(keyOwner()))
                .begin("bob")
                .begin("alice")
                .write("bob", "Account1", "100").expectSuccess()
                .read("alice", "Account1").expectNotFound()
                .commit("bob")
                .write("alice", "Account1", "200").expectRejected("Conflicting committed transaction")
                .run();

        assertTrue(result.commitTsOf("bob").compareTo(result.snapshotOf("alice")) > 0,
                "Clock push forces bob's commit above alice's snapshot");
        assertTrue(result.hlcAt(coordinatorOf("bob")).compareTo(hlc(2000)) > 0,
                "Status check RPC pushed bob's coordinator past alice's clock");
        assertEquals("100", result.committedValue("Account1"));
    }
}
