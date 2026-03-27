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
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotIsolationAnomalyTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId CLIENT_1 = ProcessId.of("client-1");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    @Test
    void snapshotIsolationAllowsWriteSkewAcrossDifferentKeys() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    InMemoryMVCCStore committedStore = new InMemoryMVCCStore();
                    committedStore.put(versionedKey("doctor-alice", ts(900)), encodeValue("on-call"));
                    committedStore.put(versionedKey("doctor-bob", ts(900)), encodeValue("on-call"));
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

            TxnId txn1 = TxnId.of("txn-write-skew-1");
            TxnId txn2 = TxnId.of("txn-write-skew-2");
            HybridTimestamp snapshotTimestamp = ts(1000);

            cluster.setTimeForProcess(CLIENT_1, 1000);
            cluster.setTimeForProcess(CLIENT_2, 1000);
            await(cluster, client1.beginTransaction(txn1, IsolationLevel.SNAPSHOT));
            await(cluster, client2.beginTransaction(txn2, IsolationLevel.SNAPSHOT));

            // MVCC/versioned values give both transactions a stable snapshot, but snapshot
            // isolation still only protects against write-write conflicts on the same key.
            // These transactions read the same invariant across two keys and then update
            // different keys, so both commits succeed and the invariant can be broken.
            cluster.setTimeForProcess(CLIENT_1, 1010);
            TxnReadResponse txn1ReadsAlice =
                    await(cluster, client1.read(txn1, "doctor-alice", snapshotTimestamp));
            cluster.setTimeForProcess(CLIENT_1, 1020);
            TxnReadResponse txn1ReadsBob =
                    await(cluster, client1.read(txn1, "doctor-bob", snapshotTimestamp));
            cluster.setTimeForProcess(CLIENT_2, 1030);
            TxnReadResponse txn2ReadsAlice =
                    await(cluster, client2.read(txn2, "doctor-alice", snapshotTimestamp));
            cluster.setTimeForProcess(CLIENT_2, 1040);
            TxnReadResponse txn2ReadsBob =
                    await(cluster, client2.read(txn2, "doctor-bob", snapshotTimestamp));

            assertEquals("on-call", txn1ReadsAlice.value());
            assertEquals("on-call", txn1ReadsBob.value());
            assertEquals("on-call", txn2ReadsAlice.value());
            assertEquals("on-call", txn2ReadsBob.value());

            cluster.setTimeForProcess(CLIENT_1, 1100);
            TxnWriteResponse txn1Write =
                    await(cluster, client1.write(txn1, "doctor-alice", "off-call"));
            cluster.setTimeForProcess(CLIENT_2, 1110);
            TxnWriteResponse txn2Write =
                    await(cluster, client2.write(txn2, "doctor-bob", "off-call"));

            assertTrue(txn1Write.success());
            assertTrue(txn2Write.success());

            cluster.setTimeForProcess(CLIENT_1, 1200);
            cluster.setTimeForProcess(CLIENT_2, 1210);
            CommitTransactionResponse txn1Commit = await(cluster, client1.commit(txn1));
            CommitTransactionResponse txn2Commit = await(cluster, client2.commit(txn2));

            assertTrue(txn1Commit.success());
            assertTrue(txn2Commit.success());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE);

            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "doctor-alice", ts(5000))
                            .filter("off-call"::equals)
                            .isPresent());
            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "doctor-bob", ts(5000))
                            .filter("off-call"::equals)
                            .isPresent());

            assertEquals(Optional.of("off-call"), committedValue(replica.committedStore(), "doctor-alice", ts(5000)));
            assertEquals(Optional.of("off-call"), committedValue(replica.committedStore(), "doctor-bob", ts(5000)));
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
