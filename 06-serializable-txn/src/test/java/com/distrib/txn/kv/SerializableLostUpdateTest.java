package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerializableLostUpdateTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId CLIENT_1 = ProcessId.of("client-1");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    @Test
    void committedWriteAfterSnapshotPreventsLostUpdateOnSameKey() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    InMemoryMVCCStore committedStore = new InMemoryMVCCStore();
                    committedStore.put(versionedKey("account-balance", ts(900)), encodeValue("100"));
                    return new SerializableTransactionalStorageReplica(
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
            TxnId txn2 = TxnId.of("txn-lost-update-0");
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

            // Serializable mode already uses read provisional records to break ties between
            // overlapping read/write transactions. Choose txn ids so txn2 wins that earlier
            // tie-break, and txn1's later write is rejected by the committed-after-snapshot
            // check added in this module.
            assertTrue(txn2Write.success());

            cluster.setTimeForProcess(CLIENT_2, 1200);
            CommitTransactionResponse txn2Commit = await(cluster, client2.commit(txn2));
            assertTrue(txn2Commit.success());

            SerializableTransactionalStorageReplica replica =
                    (SerializableTransactionalStorageReplica) cluster.getProcess(STORAGE_NODE);

            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "account-balance", ts(5000))
                            .filter("80"::equals)
                            .isPresent());

            cluster.setTimeForProcess(CLIENT_1, 1300);
            TxnWriteResponse txn1Write =
                    await(cluster, client1.write(txn1, "account-balance", "70"));

            assertFalse(txn1Write.success());
            assertEquals("Conflicting committed transaction", txn1Write.error());
            assertEquals(Optional.of("80"), committedValue(replica.committedStore(), "account-balance", ts(5000)));
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

    private Optional<String> committedValue(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).map(OrderPreservingCodec::decodeString);
    }

    private static MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(OrderPreservingCodec.encodeString(key), timestamp);
    }

    private static byte[] encodeValue(String value) {
        return OrderPreservingCodec.encodeString(value);
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }
}
