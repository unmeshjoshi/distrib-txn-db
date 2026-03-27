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
import java.util.Set;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

class TransactionalStorageReplicaClusterTest {
    private static final ProcessId STORAGE_NODE_1 = ProcessId.of("storage-node-1");
    private static final ProcessId STORAGE_NODE_2 = ProcessId.of("storage-node-2");
    private static final ProcessId STORAGE_NODE_3 = ProcessId.of("storage-node-3");
    private static final ProcessId CLIENT = ProcessId.of("client");

    @Test
    void beginTransactionCreatesPendingTxnRecordOnCoordinator() throws Exception {
        List<ProcessId> storageNodes = List.of(STORAGE_NODE_1, STORAGE_NODE_2, STORAGE_NODE_3);

        try (Cluster cluster = new Cluster()
                .withProcessIds(storageNodes)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TxnId txnId = TxnId.of("txn-1");

            BeginTransactionResponse response = await(
                    cluster,
                    client.beginTransaction(txnId, IsolationLevel.SNAPSHOT, ts(1000))
            );

            assertTrue(response.success());
            assertNotNull(response.propagatedTime());
            assertTrue(response.propagatedTime().compareTo(ts(1000)) >= 0);

            ProcessId coordinator = client.coordinatorFor(txnId);
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(coordinator);
            TxnRecord txnRecord = replica.txnRecords().get(txnId);

            assertNotNull(txnRecord);
            assertEquals(TxnStatus.PENDING, txnRecord.status());
            assertEquals(IsolationLevel.SNAPSHOT, txnRecord.isolationLevel());
            assertEquals(response.propagatedTime(), txnRecord.lastHeartbeat());
            assertTrue(txnRecord.participantReplicas().isEmpty());
        }
    }

    @Test
    void txnWriteStoresIntentAndReadReturnsOwnIntent() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE_1))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TxnId txnId = TxnId.of("txn-2");
            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT, ts(1000)));

            TxnWriteResponse writeResponse = await(
                    cluster,
                    client.write(txnId, "account-101", "1000", ts(1100), ts(1100))
            );

            assertTrue(writeResponse.success());
            assertEquals(ts(1100, 1), writeResponse.propagatedTime());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);

            assertTrue(intentExists(replica.intentStore(), "account-101", writeResponse.propagatedTime()));
            assertTrue(committedValue(replica.committedStore(), "account-101", ts(5000)).isEmpty());

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(txnId, "account-101", writeResponse.propagatedTime(), ts(1200))
            );

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertEquals(ts(1200, 1), readResponse.propagatedTime());
        }
    }

    @Test
    void txnReadSeesOwnWriteEvenWhenSnapshotTimestampIsOlder() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE_1))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TxnId txnId = TxnId.of("txn-2b");
            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT, ts(1000)));

            await(cluster, client.write(txnId, "account-101", "1000", ts(1000), ts(1200)));

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(txnId, "account-101", ts(1000), ts(1300))
            );

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
        }
    }

    @Test
    void txnReadFallsBackToCommittedStoreAtReadTimestamp() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE_1))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    InMemoryMVCCStore committedStore = new InMemoryMVCCStore();
                    committedStore.put(versionedKey("account-101", ts(900)), encodeValue("750"));
                    return new TransactionalStorageReplica(
                            committedStore,
                            new InMemoryMVCCStore(),
                            peerIds,
                            params
                    );
                })
                .start()) {

            TransactionalStorageClient client = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TxnId txnId = TxnId.of("txn-3");
            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT, ts(1000)));

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(txnId, "account-101", ts(1000), ts(1100))
            );

            assertTrue(readResponse.found());
            assertEquals("750", readResponse.value());
            assertEquals(ts(1100, 1), readResponse.propagatedTime());
        }
    }

    @Test
    void commitMovesIntentToCommittedStoreAndMarksTransactionCommitted() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE_1))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TxnId txnId = TxnId.of("txn-4");
            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT, ts(1000)));
            await(cluster, client.write(txnId, "account-101", "1000", ts(1000), ts(1100)));

            CommitTransactionResponse commitResponse =
                    await(cluster, client.commit(txnId, ts(1300)));

            assertTrue(commitResponse.success());
            assertNotNull(commitResponse.commitTimestamp());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);
            TxnRecord txnRecord = replica.txnRecords().get(txnId);

            assertNotNull(txnRecord);
            assertEquals(TxnStatus.COMMITTED, txnRecord.status());
            assertEquals(commitResponse.commitTimestamp(), txnRecord.commitTimestamp());
            assertEquals(Set.of(STORAGE_NODE_1), txnRecord.participantReplicas());

            assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "account-101", ts(5000))
                            .filter("1000"::equals)
                            .isPresent()
            );
            assertEventually(cluster, () -> !intentExists(replica.intentStore(), "account-101", ts(5000)));
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

    private Optional<String> committedValue(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).map(OrderPreservingCodec::decodeString);
    }

    private boolean intentExists(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).isPresent();
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

    private static HybridTimestamp ts(long wallClockTime, int ticks) {
        return new HybridTimestamp(wallClockTime, ticks);
    }
}
