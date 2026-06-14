package com.distrib.txn.kv.dsl;

import clock.HybridTimestamp;
import com.distrib.txn.kv.BeginTransactionResponse;
import com.distrib.txn.kv.IsolationLevel;
import com.distrib.txn.kv.TransactionalStorageClient;
import com.distrib.txn.kv.TransactionalStorageReplica;
import com.distrib.txn.kv.TxnId;
import com.distrib.txn.kv.TxnWriteResponse;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.dsl.semanticmodel.Scenario;
import kv.MVCCKey;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lost Update Scenario 1: Naturally Separated Snapshots.
 *
 * Alice (fast clock) writes and commits. Bob (slow clock, isolated coordinator)
 * reads a stale snapshot then tries to overwrite — rejected by first-committer-wins.
 */
public class LostUpdateSeparatedSnapshotsDslTest {

    private static final ProcessId NODE1 = ProcessId.of("node1");
    private static final ProcessId NODE2 = ProcessId.of("node2");
    private static final ProcessId NODE3 = ProcessId.of("node3");
    private static final List<ProcessId> SERVERS = List.of(NODE1, NODE2, NODE3);

    private static final ProcessId ALICE = ProcessId.of("alice");
    private static final ProcessId BOB = ProcessId.of("bob");

    @Test
    void fastCommitterWins_staleReaderWriteRejected() throws Exception {
        // alice is first client → coord1, bob is second → coord2.
        // Constraint: bob's coord must be off the key owner and off alice's coord,
        // so HLC merges from alice's intent don't drag bob's snapshot forward.
        Routing routing = Routing.find(SERVERS,
                r -> !r.coord2().equals(r.keyOwner())
                  && !r.coord2().equals(r.coord1()));

        TxnId aliceTxn = routing.txnId1();
        TxnId bobTxn = routing.txnId2();
        String sharedKey = routing.key();

        HybridTimestamp aliceSeed = new HybridTimestamp(2_000L, 0);
        HybridTimestamp bobSeed = new HybridTimestamp(1_000L, 0);

        AtomicReference<HybridTimestamp> aliceCommitTs = new AtomicReference<>();
        AtomicReference<HybridTimestamp> bobSnapshot = new AtomicReference<>();
        AtomicReference<HybridTimestamp> bobCoordHlc = new AtomicReference<>();
        AtomicReference<String> committedValue = new AtomicReference<>();

        Scenario<TransactionalStorageClient> s = TxnScenarios.scenario("LostUpdate separated snapshots")
                .servers(NODE1, NODE2, NODE3)
                .clients(ALICE, BOB)
                .given(g -> g.clientHlc(ALICE, aliceSeed).clientHlc(BOB, bobSeed))
                .steps(steps -> {
                    steps.client(ALICE).beginTransaction(aliceTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(ALICE).writes(aliceTxn, sharedKey, "100")
                         .expectResponse(TxnWriteResponse::success);

                    // Bob's snapshot is captured here — alice's write hasn't committed yet,
                    // so bob's snapshot stays naturally below alice's eventual commitTs.
                    steps.client(BOB).beginTransaction(bobTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(r -> {
                             bobSnapshot.set(r.propagatedTime());
                             return r.success();
                         });

                    steps.client(BOB).reads(bobTxn, sharedKey)
                         .expectResponse(r -> !r.found());

                    steps.client(ALICE).commits(aliceTxn)
                         .expectResponse(r -> {
                             aliceCommitTs.set(r.commitTimestamp());
                             return r.success();
                         });

                    // Wait for the committed version to land at the key owner so the next
                    // write hits the clean SI rejection path, not the resolve-on-read path.
                    steps.await(cluster -> hasCommittedValue(cluster, sharedKey));

                    steps.client(BOB).writes(bobTxn, sharedKey, "200")
                         .expectResponse(r -> !r.success()
                                 && "Conflicting committed transaction".equals(r.error()));

                    // Snapshot cluster state at the end of the scenario for post-run assertions.
                    steps.await(cluster -> {
                        TransactionalStorageReplica bobCoord =
                                (TransactionalStorageReplica) cluster.getProcess(routing.coord2());
                        bobCoordHlc.set(bobCoord.hybridClock().now());
                        committedValue.set(readCommittedValue(cluster, routing.keyOwner(), sharedKey));
                        return true;
                    });
                });

        s.run();

        assertTrue(aliceCommitTs.get().compareTo(bobSnapshot.get()) > 0,
                "Alice's commit must be above bob's snapshot");
        assertTrue(bobCoordHlc.get().compareTo(new HybridTimestamp(1_500L, 0)) < 0,
                "Bob's coordinator was never pushed — its HLC stayed low");
        assertEquals("100", committedValue.get());
    }

    private static boolean hasCommittedValue(Cluster cluster, String key) {
        HybridTimestamp upperBound = new HybridTimestamp(Long.MAX_VALUE, 0);
        MVCCKey lookup = new MVCCKey(OrderPreservingCodec.encodeString(key), upperBound);
        for (ProcessId node : SERVERS) {
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(node);
            if (replica.committedStore().getAsOf(lookup).isPresent()) {
                return true;
            }
        }
        return false;
    }

    private static String readCommittedValue(Cluster cluster, ProcessId keyOwner, String key) {
        TransactionalStorageReplica replica =
                (TransactionalStorageReplica) cluster.getProcess(keyOwner);
        MVCCKey lookup = new MVCCKey(
                OrderPreservingCodec.encodeString(key),
                new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE));
        return replica.committedStore().getAsOf(lookup)
                .map(OrderPreservingCodec::decodeString)
                .orElse(null);
    }
}
