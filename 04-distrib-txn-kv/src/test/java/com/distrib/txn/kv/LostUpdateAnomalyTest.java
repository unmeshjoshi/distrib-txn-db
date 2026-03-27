package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LostUpdateAnomalyTest extends TransactionalStorageReplicaTestSupport {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId CLIENT_1 = ProcessId.of("client-1");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    @Test
    void snapshotIsolationStillAllowsLostUpdateAfterEarlierConflictingCommitIsResolved() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    InMemoryMVCCStore committedStore = new InMemoryMVCCStore();
                    committedStore.put(versionedKey("account-balance", ts(900)), encodeValue("100"));
                    return new TransactionalStorageReplica(
                            committedStore,
                            new InMemoryMVCCStore(),
                            peerIds,
                            params
                    );
                })
                .start()) {

            TransactionalStorageClient client1 = cluster.newClient(CLIENT_1, TransactionalStorageClient::new);
            TransactionalStorageClient client2 = cluster.newClient(CLIENT_2, TransactionalStorageClient::new);

            TxnId txn1 = TxnId.of("txn-lost-update-1");
            TxnId txn2 = TxnId.of("txn-lost-update-2");
            HybridTimestamp snapshotTimestamp = ts(1000);

            cluster.setTimeForProcess(CLIENT_1, 1000);
            cluster.setTimeForProcess(CLIENT_2, 1000);
            await(cluster, client1.beginTransaction(txn1, IsolationLevel.SNAPSHOT));
            await(cluster, client2.beginTransaction(txn2, IsolationLevel.SNAPSHOT));

            cluster.setTimeForProcess(CLIENT_1, 1010);
            TxnReadResponse txn1ReadsBalance =
                    await(cluster, client1.read(txn1, "account-balance", snapshotTimestamp));
            cluster.setTimeForProcess(CLIENT_2, 1020);
            TxnReadResponse txn2ReadsBalance =
                    await(cluster, client2.read(txn2, "account-balance", snapshotTimestamp));

            assertEquals("100", txn1ReadsBalance.value());
            assertEquals("100", txn2ReadsBalance.value());

            cluster.setTimeForProcess(CLIENT_2, 1100);
            TxnWriteResponse txn2Write =
                    await(cluster, client2.write(txn2, "account-balance", "80"));
            assertTrue(txn2Write.success());

            cluster.setTimeForProcess(CLIENT_2, 1200);
            CommitTransactionResponse txn2Commit = await(cluster, client2.commit(txn2));
            assertTrue(txn2Commit.success());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE);

            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "account-balance", ts(5000))
                            .filter("80"::equals)
                            .isPresent());

            // Textbook snapshot isolation would reject txn1's later write because the same key
            // was already committed after txn1's snapshot timestamp. This module does not yet
            // perform that committed-version-after-readTimestamp validation, so txn1 is still
            // allowed to overwrite the newer committed value.
            cluster.setTimeForProcess(CLIENT_1, 1300);
            TxnWriteResponse txn1Write =
                    await(cluster, client1.write(txn1, "account-balance", "70"));
            assertTrue(txn1Write.success());

            cluster.setTimeForProcess(CLIENT_1, 1400);
            CommitTransactionResponse txn1Commit = await(cluster, client1.commit(txn1));
            assertTrue(txn1Commit.success());

            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "account-balance", ts(5000))
                            .filter("70"::equals)
                            .isPresent());

            assertEquals(Optional.of("70"), committedValue(replica.committedStore(), "account-balance", ts(5000)));
        }
    }
}
