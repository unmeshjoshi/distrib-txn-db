package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lost Update Scenario 1: Naturally Separated Snapshots.
 *
 * A fast-clock client writes and commits a key. A slow-clock client, whose
 * coordinator was never touched by the fast client's operations, reads a stale
 * snapshot and then attempts to overwrite the same key. Snapshot Isolation's
 * first-committer-wins rule rejects the stale write because a committed version
 * exists after the slow client's read timestamp.
 *
 * Topology constraint:
 *   - The stale reader's coordinator must be on a different node from both the
 *     key owner and the fast committer's coordinator, so its clock stays low.
 */
class LostUpdateSeparatedSnapshotsTest extends TransactionalStorageReplicaTestSupport {
    private static final ProcessId FAST_CLOCK_CLIENT = ProcessId.of("fast-clock-client");
    private static final ProcessId SLOW_CLOCK_CLIENT = ProcessId.of("slow-clock-client");

    @Test
    void fastCommitterWins_staleReaderWriteRejected() throws Exception {
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

            // Find a topology where the stale reader's coordinator is untouched
            // by the fast committer's operations.
            TopologyScenario topology = findTopology(fastClient, s ->
                    !s.coordinator2().equals(s.keyOwner()) &&
                    !s.coordinator2().equals(s.coordinator1()));

            String sharedKey = topology.key();
            TxnId fastTxnId = topology.txnId1();
            TxnId staleTxnId = topology.txnId2();

            // Step 1: Fast-clock client begins its transaction.
            // Its coordinator receives the high client time and assigns a high readTs.
            cluster.setTimeForProcess(FAST_CLOCK_CLIENT, 2000);
            cluster.setTimeForProcess(SLOW_CLOCK_CLIENT, 1000);

            BeginTransactionResponse fastBegin = cluster.tickUntilComplete(
                    fastClient.beginTransaction(fastTxnId, IsolationLevel.SNAPSHOT));
            assertTrue(fastBegin.success());
            HybridTimestamp fastSnapshot = fastBegin.propagatedTime();

            // Step 2: Fast-clock client writes the shared key.
            TxnWriteResponse fastWrite = cluster.tickUntilComplete(
                    fastClient.write(fastTxnId, sharedKey, "100"));
            assertTrue(fastWrite.success());

            // Step 3: Slow-clock client begins its transaction on a different,
            // untouched coordinator. Its readTs stays low.
            BeginTransactionResponse slowBegin = cluster.tickUntilComplete(
                    slowClient.beginTransaction(staleTxnId, IsolationLevel.SNAPSHOT));
            assertTrue(slowBegin.success());
            HybridTimestamp slowSnapshot = slowBegin.propagatedTime();

            assertTrue(slowSnapshot.compareTo(fastSnapshot) < 0,
                    "Slow client's snapshot (" + slowSnapshot + ") must be below fast client's snapshot (" + fastSnapshot + ")");

            // Step 4: Slow-clock client reads the shared key.
            // It encounters the fast client's pending intent, checks its status (PENDING),
            // and falls through to reading committed data (none exists yet).
            TxnReadResponse slowRead = cluster.tickUntilComplete(
                    slowClient.read(staleTxnId, sharedKey));
            assertFalse(slowRead.found());

            // Step 5: Fast-clock client commits.
            // The committed version lands at a timestamp well above the slow client's snapshot.
            CommitTransactionResponse fastCommit = cluster.tickUntilComplete(
                    fastClient.commit(fastTxnId));
            assertTrue(fastCommit.success());
            HybridTimestamp fastCommitTs = fastCommit.commitTimestamp();

            assertTrue(fastCommitTs.compareTo(slowSnapshot) > 0,
                    "Fast commit (" + fastCommitTs + ") must be above slow snapshot (" + slowSnapshot + ")");

            // Wait for the resolve to finalize the committed value on the key owner.
            TransactionalStorageReplica keyOwnerReplica =
                    (TransactionalStorageReplica) cluster.getProcess(topology.keyOwner());
            cluster.tickUntil(() ->
                    committedValue(keyOwnerReplica.committedStore(), sharedKey, ts(Long.MAX_VALUE))
                            .filter("100"::equals)
                            .isPresent());

            // Step 6: Slow-clock client attempts to write the same key based on
            // its stale read. First-committer-wins rejects this write.
            TxnWriteResponse staleWrite = cluster.tickUntilComplete(
                    slowClient.write(staleTxnId, sharedKey, "200"));
            assertFalse(staleWrite.success(),
                    "SI validation must reject: committed version (" + fastCommitTs
                            + ") > stale snapshot (" + slowSnapshot + ")");
            assertEquals("Conflicting committed transaction", staleWrite.error());
        }
    }
}
