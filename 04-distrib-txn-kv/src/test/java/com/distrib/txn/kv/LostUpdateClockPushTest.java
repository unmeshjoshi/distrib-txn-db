package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.distrib.txn.kv.dsl.TopologyScenario;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lost Update Scenario 2: HLC Propagation Pushes a Lagging Commit Forward.
 *
 * A slow-clock client writes an intent first. A fast-clock client reads the key,
 * encounters the pending intent, and triggers a GetTransactionStatus RPC to the
 * slow writer's coordinator. This RPC propagates the fast client's high HLC
 * timestamp across the network to the writer's coordinator, pushing its clock
 * forward. When the slow writer commits, its commit timestamp is forced above
 * the fast reader's snapshot, and the fast reader's subsequent write is rejected
 * by the first-committer-wins rule.
 *
 * Topology constraint:
 *   - All three roles (key owner, writer's coordinator, reader's coordinator)
 *     must be on different nodes, so the clock push is a true cross-node RPC.
 */
class LostUpdateClockPushTest extends TransactionalStorageReplicaTestSupport {
    private static final ProcessId FAST_CLOCK_CLIENT = ProcessId.of("fast-clock-client");
    private static final ProcessId SLOW_CLOCK_CLIENT = ProcessId.of("slow-clock-client");

    @Test
    void hlcPropagationPushesLaggingCommitAboveLeadingSnapshot() throws Exception {
        List<ProcessId> storageNodes = List.of(STORAGE_NODE_1, STORAGE_NODE_2, STORAGE_NODE_3);

        try (Cluster cluster = new Cluster()
                .withProcessIds(storageNodes)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params
                ))
                .start()) {

            var fastClient =
                    cluster.newClient(FAST_CLOCK_CLIENT, TransactionalStorageClient::new);
            var slowClient =
                    cluster.newClient(SLOW_CLOCK_CLIENT, TransactionalStorageClient::new);

            // Find a topology where all three roles land on different nodes:
            //   txnId1 = slow writer, coordinator1 = writer's coordinator
            //   txnId2 = fast reader, coordinator2 = reader's coordinator
            //   keyOwner = the node that stores the key
            TopologyScenario topology = findTopology(fastClient, s ->
                    !s.coordinator1().equals(s.keyOwner()) &&
                    !s.coordinator2().equals(s.keyOwner()) &&
                    !s.coordinator1().equals(s.coordinator2()));

            String sharedKey = topology.key();
            TxnId writerTxnId = topology.txnId1();
            TxnId readerTxnId = topology.txnId2();

            cluster.setTimeForProcess(FAST_CLOCK_CLIENT, 2000);
            cluster.setTimeForProcess(SLOW_CLOCK_CLIENT, 1000);

            // Step 1: Slow-clock client begins its transaction.
            // Its coordinator is on a node with a low clock.
            BeginTransactionResponse slowBegin = cluster.tickUntilComplete(
                    slowClient.beginTransaction(writerTxnId, IsolationLevel.SNAPSHOT));
            assertTrue(slowBegin.success());

            // Step 2: Fast-clock client begins its transaction on a different coordinator.
            BeginTransactionResponse fastBegin = cluster.tickUntilComplete(
                    fastClient.beginTransaction(readerTxnId, IsolationLevel.SNAPSHOT));
            assertTrue(fastBegin.success());
            HybridTimestamp fastSnapshot = fastBegin.propagatedTime();

            // Step 3: Slow-clock client writes the shared key.
            // The intent is stored at a low timestamp on the key owner node.
            TxnWriteResponse slowWrite = cluster.tickUntilComplete(
                    slowClient.write(writerTxnId, sharedKey, "100"));
            assertTrue(slowWrite.success());
            assertTrue(slowWrite.propagatedTime().compareTo(fastSnapshot) < 0,
                    "Writer's intent timestamp must be below the reader's snapshot");

            // Step 4: Fast-clock client reads the shared key.
            // The read arrives at the key owner, which sees the pending intent from
            // the slow writer. The key owner sends a GetTransactionStatus RPC to the
            // writer's coordinator (a different node), carrying the fast client's
            // high HLC timestamp. This pushes the writer's coordinator clock forward.
            // The status is PENDING, so the read ignores the intent.
            TxnReadResponse fastRead = cluster.tickUntilComplete(
                    fastClient.read(readerTxnId, sharedKey));
            assertFalse(fastRead.found());

            // Step 5: Slow-clock client commits.
            // Its coordinator's clock was pushed forward by the status check in Step 4.
            // The commit timestamp is therefore forced above the fast reader's snapshot.
            CommitTransactionResponse slowCommit = cluster.tickUntilComplete(
                    slowClient.commit(writerTxnId));
            assertTrue(slowCommit.success());
            HybridTimestamp slowCommitTs = slowCommit.commitTimestamp();

            assertTrue(slowCommitTs.compareTo(fastSnapshot) > 0,
                    "Clock push forces slow commit (" + slowCommitTs
                            + ") above fast snapshot (" + fastSnapshot + ")");

            // Wait for the resolve to finalize the committed value.
            TransactionalStorageReplica keyOwnerReplica =
                    (TransactionalStorageReplica) cluster.getProcess(topology.keyOwner());
            cluster.tickUntil(() ->
                    committedValue(keyOwnerReplica.committedStore(), sharedKey, ts(Long.MAX_VALUE))
                            .filter("100"::equals)
                            .isPresent());

            // Step 6: Fast-clock client attempts to write the same key.
            // First-committer-wins rejects this: the slow writer's committed version
            // is now above the fast reader's snapshot.
            TxnWriteResponse staleWrite = cluster.tickUntilComplete(
                    fastClient.write(readerTxnId, sharedKey, "200"));
            assertFalse(staleWrite.success(),
                    "SI validation must reject: committed version (" + slowCommitTs
                            + ") > snapshot (" + fastSnapshot + ")");
            assertEquals("Conflicting committed transaction", staleWrite.error());
        }
    }
}
