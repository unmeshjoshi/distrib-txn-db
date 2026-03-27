package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import com.tickloom.util.Timeout;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

class TransactionalStorageReplicaClusterTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ProcessId STORAGE_NODE_1 = ProcessId.of("storage-node-1");
    private static final ProcessId STORAGE_NODE_2 = ProcessId.of("storage-node-2");
    private static final ProcessId STORAGE_NODE_3 = ProcessId.of("storage-node-3");
    private static final ProcessId CLIENT = ProcessId.of("client");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

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
            assertTrue(txnRecord.heartbeatTimeout().isTicking());
            assertEquals(10000, txnRecord.heartbeatTimeout().getDurationTicks());
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
    void txnReadIgnoresForeignPendingIntentAndReturnsCommittedValue() throws Exception {
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
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);

            TxnId pendingTxn = TxnId.of("txn-read-pending");
            replica.txnRecords().put(pendingTxn, new TxnRecord(
                    pendingTxn,
                    TxnStatus.PENDING,
                    ts(1000),
                    null,
                    Set.of(STORAGE_NODE_1),
                    startedTimeout(pendingTxn),
                    IsolationLevel.SNAPSHOT
            ));
            replica.intentStore().put(
                    versionedKey("account-101", ts(1100)),
                    encodeIntentRecord(new IntentRecord(pendingTxn, "1000"))
            );

            TxnId readerTxn = TxnId.of("txn-read-visible");
            await(cluster, client.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT, ts(1000)));

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(readerTxn, "account-101", ts(1000), ts(1200))
            );

            assertTrue(readResponse.found());
            assertEquals("750", readResponse.value());
            assertTrue(intentExists(replica.intentStore(), "account-101", ts(5000)));
        }
    }

    @Test
    void txnReadResolvesCommittedForeignIntentBeforeReturningValue() throws Exception {
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
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);

            TxnId committedTxn = TxnId.of("txn-read-committed");
            HybridTimestamp intentTimestamp = ts(1100);
            HybridTimestamp commitTimestamp = ts(1200);

            replica.txnRecords().put(committedTxn, new TxnRecord(
                    committedTxn,
                    TxnStatus.COMMITTED,
                    ts(1000),
                    commitTimestamp,
                    Set.of(STORAGE_NODE_1),
                    startedTimeout(committedTxn),
                    IsolationLevel.SNAPSHOT
            ));
            replica.intentStore().put(
                    versionedKey("account-101", intentTimestamp),
                    encodeIntentRecord(new IntentRecord(committedTxn, "1000"))
            );

            TxnId readerTxn = TxnId.of("txn-read-after-commit");
            await(cluster, client.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT, ts(1300)));

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(readerTxn, "account-101", ts(1300), ts(1400))
            );

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertEquals(
                    Optional.of("1000"),
                    committedValue(replica.committedStore(), "account-101", ts(5000))
            );
            assertFalse(intentExists(replica.intentStore(), "account-101", ts(5000)));
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

    @Test
    void txnWriteFailsWhenForeignIntentTransactionIsStillPending() throws Exception {
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

            TxnId txn1 = TxnId.of("txn-5a");
            await(cluster, client.beginTransaction(txn1, IsolationLevel.SNAPSHOT, ts(1000)));
            await(cluster, client.write(txn1, "account-101", "1000", ts(1000), ts(1100)));

            TxnId txn2 = TxnId.of("txn-5b");
            await(cluster, client.beginTransaction(txn2, IsolationLevel.SNAPSHOT, ts(1000)));
            TxnWriteResponse write2 = await(cluster, client.write(txn2, "account-101", "2000", ts(1000), ts(1200)));

            assertFalse(write2.success());
            assertEquals("Conflicting pending transaction", write2.error());
        }
    }

    @Test
    void txnWriteResolvesCommittedForeignIntentBeforeWritingNewIntent() throws Exception {
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
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);

            TxnId committedTxn = TxnId.of("txn-6a");
            HybridTimestamp foreignIntentTimestamp = ts(1100);
            HybridTimestamp foreignCommitTimestamp = ts(1200);

            replica.txnRecords().put(committedTxn, new TxnRecord(
                    committedTxn,
                    TxnStatus.COMMITTED,
                    ts(1000),
                    foreignCommitTimestamp,
                    Set.of(STORAGE_NODE_1),
                    startedTimeout(committedTxn),
                    IsolationLevel.SNAPSHOT
            ));
            replica.intentStore().put(
                    versionedKey("account-101", foreignIntentTimestamp),
                    encodeIntentRecord(new IntentRecord(committedTxn, "1000"))
            );

            TxnId writerTxn = TxnId.of("txn-6b");
            await(cluster, client.beginTransaction(writerTxn, IsolationLevel.SNAPSHOT, ts(1300)));
            TxnWriteResponse writeResponse =
                    await(cluster, client.write(writerTxn, "account-101", "2000", ts(1300), ts(1400)));

            assertTrue(writeResponse.success());

            assertEquals(
                    Optional.of("1000"),
                    committedValue(replica.committedStore(), "account-101", ts(5000))
            );

            TxnReadResponse ownRead =
                    await(cluster, client.read(writerTxn, "account-101", ts(1300), ts(1500)));
            assertTrue(ownRead.found());
            assertEquals("2000", ownRead.value());
        }
    }

    @Test
    void txnWriteResolvesLingeringCommittedIntentAfterDroppedResolveRequest() throws Exception {
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

            TransactionalStorageClient client1 = cluster.newClient(CLIENT, TransactionalStorageClient::new);
            TransactionalStorageClient client2 = cluster.newClient(CLIENT_2, TransactionalStorageClient::new);

            RoutingScenario routingScenario = routingScenario(client1);

            // Only the first transaction's resolve cleanup is dropped. Status checks for the
            // second writer still flow, and this test does not commit the second transaction.
            cluster.dropMessagesOfType(
                    routingScenario.coordinator(),
                    routingScenario.participant(),
                    TransactionalMessageTypes.RESOLVE_TRANSACTION_REQUEST
            );

            await(cluster, client1.beginTransaction(routingScenario.firstTxnId(), IsolationLevel.SNAPSHOT, ts(1000)));
            await(cluster, client1.write(routingScenario.firstTxnId(), routingScenario.key(), "1000", ts(1000), ts(1100)));

            CommitTransactionResponse firstCommit =
                    await(cluster, client1.commit(routingScenario.firstTxnId(), ts(1200)));

            assertTrue(firstCommit.success());

            TransactionalStorageReplica participantReplica =
                    (TransactionalStorageReplica) cluster.getProcess(routingScenario.participant());
            TransactionalStorageReplica coordinatorReplica =
                    (TransactionalStorageReplica) cluster.getProcess(routingScenario.coordinator());

            assertEventually(cluster, () -> {
                TxnRecord txnRecord = coordinatorReplica.txnRecords().get(routingScenario.firstTxnId());
                return txnRecord != null && txnRecord.status() == TxnStatus.COMMITTED;
            });
            assertEventually(cluster, () ->
                    intentExists(participantReplica.intentStore(), routingScenario.key(), ts(5000)));
            assertTrue(committedValue(participantReplica.committedStore(), routingScenario.key(), ts(5000)).isEmpty());

            await(cluster, client2.beginTransaction(routingScenario.secondTxnId(), IsolationLevel.SNAPSHOT, ts(1300)));
            TxnWriteResponse secondWrite = await(
                    cluster,
                    client2.write(routingScenario.secondTxnId(), routingScenario.key(), "2000", ts(1300), ts(1400))
            );

            assertTrue(secondWrite.success());
            assertEventually(cluster, () ->
                    committedValue(participantReplica.committedStore(), routingScenario.key(), ts(5000))
                            .filter("1000"::equals)
                            .isPresent());

            TxnReadResponse ownRead =
                    await(cluster, client2.read(routingScenario.secondTxnId(), routingScenario.key(), ts(1300), ts(1500)));
            assertTrue(ownRead.found());
            assertEquals("2000", ownRead.value());
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

    private static byte[] encodeIntentRecord(IntentRecord intentRecord) throws Exception {
        return OBJECT_MAPPER.writeValueAsBytes(intentRecord);
    }

    private static Timeout startedTimeout(TxnId txnId) {
        Timeout timeout = new Timeout("txn-" + txnId, 10000);
        timeout.start();
        return timeout;
    }

    private RoutingScenario routingScenario(TransactionalStorageClient client) {
        List<TxnId> txnIds = List.of(
                TxnId.of("txn-network-1"),
                TxnId.of("txn-network-2"),
                TxnId.of("txn-network-3"),
                TxnId.of("txn-network-4")
        );
        List<String> keys = List.of(
                "account-101",
                "account-202",
                "account-303",
                "account-404",
                "account-505"
        );

        for (TxnId firstTxnId : txnIds) {
            ProcessId coordinator = client.coordinatorFor(firstTxnId);
            for (String key : keys) {
                ProcessId participant = client.replicaFor(key);
                if (participant.equals(coordinator)) {
                    continue;
                }

                TxnId secondTxnId = TxnId.of(firstTxnId + "-writer-2");
                return new RoutingScenario(firstTxnId, secondTxnId, key, coordinator, participant);
            }
        }

        fail("Could not find a key and transaction id that route to different replicas.");
        throw new IllegalStateException("Unreachable");
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }

    private static HybridTimestamp ts(long wallClockTime, int ticks) {
        return new HybridTimestamp(wallClockTime, ticks);
    }

    private record RoutingScenario(
            TxnId firstTxnId,
            TxnId secondTxnId,
            String key,
            ProcessId coordinator,
            ProcessId participant
    ) {
    }
}
