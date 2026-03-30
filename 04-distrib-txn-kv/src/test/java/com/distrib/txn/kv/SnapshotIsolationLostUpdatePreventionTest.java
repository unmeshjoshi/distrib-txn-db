package com.distrib.txn.kv;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.tickUntil;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot Isolation test for the "Read Followed by Write in HLC" scenario described in
 * {@code si_hlc_vs_timestamp_oracle_spec.md}.
 *
 * This is the lost-update case discussed in {@code isolation-level.md}: two transactions write the
 * same key, and Snapshot Isolation must reject the stale writer once a newer committed version
 * exists after its snapshot timestamp.
 *
 * More specifically, this test verifies the Snapshot Isolation write-write validation invoked from
 * {@code TransactionalStorageReplica.writeItentFor(...)} via
 * {@code failsSnapshotIsolationWriteValidation(...)}. If a committed version of the same key
 * already exists after the transaction's read timestamp, the stale write must be rejected.
 */
class SnapshotIsolationLostUpdatePreventionTest extends TransactionalStorageReplicaTestSupport {
    private static final ProcessId LEADING_CLOCK_CLIENT = ProcessId.of("leading-clock-client");
    private static final ProcessId LAGGING_CLOCK_CLIENT = ProcessId.of("lagging-clock-client");
    private static final String SHARED_KEY = "x";
    private static final TxnId LEADING_CLOCK_TXN = TxnId.of("leading-txn");
    private static final TxnId LAGGING_CLOCK_TXN = TxnId.of("lagging-txn");

    @Test
    void readFollowedByWriteInHlcRejectsLostUpdateWithSiValidation() throws Exception {
        List<ProcessId> storageNodes = List.of(STORAGE_NODE_1, STORAGE_NODE_2);

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

            var leadingClockClient =
                    cluster.newClient(LEADING_CLOCK_CLIENT, TransactionalStorageClient::new);
            var laggingClockClient =
                    cluster.newClient(LAGGING_CLOCK_CLIENT, TransactionalStorageClient::new);

            assertEquals(STORAGE_NODE_1, leadingClockClient.replicaFor(SHARED_KEY));
            assertEquals(STORAGE_NODE_2, leadingClockClient.coordinatorFor(LEADING_CLOCK_TXN));
            assertEquals(STORAGE_NODE_1, leadingClockClient.coordinatorFor(LAGGING_CLOCK_TXN));

            cluster.setTimeForProcess(LEADING_CLOCK_CLIENT, 1005);
            cluster.setTimeForProcess(LAGGING_CLOCK_CLIENT, 1000);

            BeginTransactionResponse leadingReaderBegin = tickUntilComplete(
                    cluster,
                    leadingClockClient.beginTransaction(LEADING_CLOCK_TXN, IsolationLevel.SNAPSHOT)
            );
            assertTrue(leadingReaderBegin.success());

            // The leading-clock client begins first on the faster coordinator and reads x from the
            // owner node. That read pushes the owner node's HLC forward to the reader's snapshot.
            TxnReadResponse leadingReaderSeesNoValue = tickUntilComplete(
                    cluster,
                    leadingClockClient.read(LEADING_CLOCK_TXN, SHARED_KEY)
            );
            assertFalse(leadingReaderSeesNoValue.found());

            // The lagging-clock client begins later. Even though its local clock lags behind, its
            // write to the same key on the owner node is pushed forward by HLC merge and commits
            // after the leading reader's snapshot.
            var laggingWriterBegin = tickUntilComplete(
                    cluster,
                    laggingClockClient.beginTransaction(LAGGING_CLOCK_TXN, IsolationLevel.SNAPSHOT)
            );
            assertTrue(laggingWriterBegin.success());

            var laggingWriteResponse = tickUntilComplete(
                    cluster,
                    laggingClockClient.write(LAGGING_CLOCK_TXN, SHARED_KEY, "80")
            );
            assertTrue(laggingWriteResponse.success());
            assertTrue(laggingWriteResponse.propagatedTime().compareTo(leadingReaderBegin.propagatedTime()) > 0);

            var laggingWriterCommit = tickUntilComplete(
                    cluster,
                    laggingClockClient.commit(LAGGING_CLOCK_TXN)
            );
            assertTrue(laggingWriterCommit.success());
            assertTrue(laggingWriterCommit.commitTimestamp().compareTo(leadingReaderBegin.propagatedTime()) > 0);

            TransactionalStorageReplica ownerReplica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);

            tickUntil(cluster, () ->
                    committedValue(ownerReplica.committedStore(), SHARED_KEY, ts(5000))
                            .filter("80"::equals)
                            .isPresent());

            // The leading reader now tries to write the same key based on its older snapshot. The
            // owner node has already committed a newer version, so Snapshot Isolation rejects this
            // stale overwrite instead of allowing a lost update.
            TxnWriteResponse staleWriteFromLeadingReader = tickUntilComplete(
                    cluster,
                    leadingClockClient.write(LEADING_CLOCK_TXN, SHARED_KEY, "70")
            );
            assertFalse(staleWriteFromLeadingReader.success());
            assertEquals("Conflicting committed transaction", staleWriteFromLeadingReader.error());

        }
    }
}
