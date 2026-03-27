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

class SerializableWriteSkewTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId CLIENT_1 = ProcessId.of("client-1");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    @Test
    void readProvisionalRecordsPreventWriteSkewAcrossDifferentKeys() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    InMemoryMVCCStore committedStore = new InMemoryMVCCStore();
                    committedStore.put(versionedKey("doctor-alice", ts(900)), encodeValue("on-call"));
                    committedStore.put(versionedKey("doctor-bob", ts(900)), encodeValue("on-call"));
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

            TxnId txn1 = TxnId.of("txn-write-skew-1");
            TxnId txn2 = TxnId.of("txn-write-skew-2");
            HybridTimestamp snapshotTimestamp = ts(1000);

            await(cluster, client1.beginTransaction(txn1, IsolationLevel.SNAPSHOT, snapshotTimestamp));
            await(cluster, client2.beginTransaction(txn2, IsolationLevel.SNAPSHOT, snapshotTimestamp));

            // Serializable mode tracks these reads, so a later write to either key must notice
            // the conflicting foreign read and block one of the transactions.
            TxnReadResponse txn1ReadsAlice =
                    await(cluster, client1.read(txn1, "doctor-alice", snapshotTimestamp, ts(1010)));
            TxnReadResponse txn1ReadsBob =
                    await(cluster, client1.read(txn1, "doctor-bob", snapshotTimestamp, ts(1020)));
            TxnReadResponse txn2ReadsAlice =
                    await(cluster, client2.read(txn2, "doctor-alice", snapshotTimestamp, ts(1030)));
            TxnReadResponse txn2ReadsBob =
                    await(cluster, client2.read(txn2, "doctor-bob", snapshotTimestamp, ts(1040)));

            assertEquals("on-call", txn1ReadsAlice.value());
            assertEquals("on-call", txn1ReadsBob.value());
            assertEquals("on-call", txn2ReadsAlice.value());
            assertEquals("on-call", txn2ReadsBob.value());

            TxnWriteResponse txn1Write =
                    await(cluster, client1.write(txn1, "doctor-alice", "off-call", snapshotTimestamp, ts(1100)));
            TxnWriteResponse txn2Write =
                    await(cluster, client2.write(txn2, "doctor-bob", "off-call", snapshotTimestamp, ts(1110)));

            assertTrue(txn1Write.success());
            // The second transaction is stopped before commit. Its write conflicts with the
            // other transaction's read provisional record, so serializable validation fails at
            // write time rather than allowing both transactions to reach commit.
            assertFalse(txn2Write.success());

            CommitTransactionResponse txn1Commit = await(cluster, client1.commit(txn1, ts(1200)));
            assertTrue(txn1Commit.success());

            SerializableTransactionalStorageReplica replica =
                    (SerializableTransactionalStorageReplica) cluster.getProcess(STORAGE_NODE);

            // txn2's read provisional records may still exist after its write fails. They are
            // harmless here because txn2 never commits, and later writes will ignore or clean up
            // read markers that belong to non-pending transactions.
            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "doctor-alice", ts(5000))
                            .filter("off-call"::equals)
                            .isPresent());
            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "doctor-bob", ts(5000))
                            .filter("on-call"::equals)
                            .isPresent());

            assertEquals(Optional.of("off-call"), committedValue(replica.committedStore(), "doctor-alice", ts(5000)));
            assertEquals(Optional.of("on-call"), committedValue(replica.committedStore(), "doctor-bob", ts(5000)));
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
