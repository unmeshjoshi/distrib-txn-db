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
 * Lost Update Scenario 2: HLC Propagation Pushes a Lagging Commit Forward.
 *
 * Bob (slow clock) writes first. Alice (fast clock) reads, pushing Bob's coordinator
 * clock forward via the status check RPC. Bob commits at a forced-high timestamp.
 * Alice's write is rejected by first-committer-wins.
 */
public class LostUpdateClockPushDslTest {

    private static final ProcessId NODE1 = ProcessId.of("node1");
    private static final ProcessId NODE2 = ProcessId.of("node2");
    private static final ProcessId NODE3 = ProcessId.of("node3");
    private static final List<ProcessId> SERVERS = List.of(NODE1, NODE2, NODE3);

    private static final ProcessId BOB = ProcessId.of("bob");
    private static final ProcessId ALICE = ProcessId.of("alice");

    @Test
    void hlcPropagationPushesLaggingCommitAboveLeadingSnapshot() throws Exception {
        // bob is first client → coord1, alice is second → coord2.
        // Constraint: alice and bob transactions both should use different coordinator nodes
        // which should also be different from the node storing the key
        // in the transaction. This demonstrates that even with coordinators
        //on different nodes, the hybridclock propogation pushes the
        //commit timestamp.
        Routing routing = Routing.find(SERVERS,
                r -> !r.coord1().equals(r.keyOwner())
                  && !r.coord1().equals(r.coord2())
                  && !r.coord2().equals(r.keyOwner()));

        TxnId bobTxn = routing.txnId1();
        TxnId aliceTxn = routing.txnId2();
        String sharedKey = routing.key();

        HybridTimestamp bobLaggingClock = new HybridTimestamp(1_000L, 0);
        HybridTimestamp aliceLeadingClock = new HybridTimestamp(2_000L, 0);

        //atomic just so that it can be referred in the lambda.
        AtomicReference<HybridTimestamp> bobCommitTs = new AtomicReference<>();
        AtomicReference<HybridTimestamp> aliceSnapshot = new AtomicReference<>();
        AtomicReference<HybridTimestamp> bobCoordHlc = new AtomicReference<>();
        AtomicReference<String> committedValue = new AtomicReference<>();

        Scenario<TransactionalStorageClient> s = TxnScenarios.scenario("LostUpdate HLC clock push")
                .servers(NODE1, NODE2, NODE3)
                .clients(BOB, ALICE)
                .given(g -> g.clientHlc(BOB, bobLaggingClock).clientHlc(ALICE, aliceLeadingClock))
                .steps(steps -> {
                    steps.client(BOB).beginTransaction(bobTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(ALICE).beginTransaction(aliceTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(r -> {
                             aliceSnapshot.set(r.propagatedTime());
                             return r.success();
                         });

                    steps.client(BOB).writes(bobTxn, sharedKey, "100")
                         .expectResponse(TxnWriteResponse::success);

                    // Alice's read crosses to bob's coordinator via the status check RPC,
                    // dragging bob's coordinator HLC up above alice's clock.
                    steps.client(ALICE).reads(aliceTxn, sharedKey)
                         .expectResponse(r -> !r.found());

                    // Bob's commit picks a timestamp from the now-pushed coordinator HLC,
                    // landing above alice's snapshot.
                    steps.client(BOB).commits(bobTxn)
                         .expectResponse(r -> {
                             bobCommitTs.set(r.commitTimestamp());
                             return r.success();
                         });

                    // Wait for the committed version to land at the key owner so alice's
                    // losing write hits the clean SI rejection path.
                    steps.await(cluster -> hasCommittedValue(cluster, sharedKey));

                    steps.client(ALICE).writes(aliceTxn, sharedKey, "200")
                         .expectResponse(r -> !r.success()
                                 && "Conflicting committed transaction".equals(r.error()));

                    // Snapshot cluster state at the end of the scenario for post-run assertions.
                    steps.await(cluster -> {
                        TransactionalStorageReplica bobCoord =
                                (TransactionalStorageReplica) cluster.getProcess(routing.coord1());
                        bobCoordHlc.set(bobCoord.hybridClock().now());
                        committedValue.set(readCommittedValue(cluster, routing.keyOwner(), sharedKey));
                        return true;
                    });
                });

        s.run();

        assertTrue(bobCommitTs.get().compareTo(aliceSnapshot.get()) > 0,
                "Clock push forces bob's commit above alice's snapshot");
        assertTrue(bobCoordHlc.get().compareTo(new HybridTimestamp(2_000L, 0)) > 0,
                "Status check RPC pushed bob's coordinator past alice's clock");
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
