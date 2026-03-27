package com.distrib.txn.kv;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TransactionalStorageReplicaCoreFlowTest extends TransactionalStorageReplicaTestSupport {
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
            cluster.setTimeForProcess(CLIENT, 1000);

            BeginTransactionResponse response = await(
                    cluster,
                    client.beginTransaction(txnId, IsolationLevel.SNAPSHOT)
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
            // The coordinator only knows the participant set once the client sends commit with
            // the replicas it actually wrote to. At begin time, no reads or writes have happened
            // yet, so the transaction record starts with an empty participant set.
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
            cluster.setTimeForProcess(CLIENT, 1000);
            BeginTransactionResponse beginResponse =
                    await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT));

            cluster.setTimeForProcess(CLIENT, 1100);

            TxnWriteResponse writeResponse = await(
                    cluster,
                    client.write(txnId, "account-101", "1000")
            );

            assertTrue(writeResponse.success());
            assertEquals(ts(1100, 1), writeResponse.propagatedTime());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);
            assertTrue(intentExists(replica.intentStore(), "account-101", writeResponse.propagatedTime()));
            assertTrue(committedValue(replica.committedStore(), "account-101", ts(5000)).isEmpty());

            cluster.setTimeForProcess(CLIENT, 1200);
            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(txnId, "account-101", writeResponse.propagatedTime())
            );

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertEquals(ts(1200, 1), readResponse.propagatedTime());
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
            cluster.setTimeForProcess(CLIENT, 1000);

            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT));

            await(cluster, client.write(txnId, "account-101", "1000"));

            CommitTransactionResponse commitResponse =
                    await(cluster, client.commit(txnId));

            assertTrue(commitResponse.success());
            assertNotNull(commitResponse.commitTimestamp());

            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(STORAGE_NODE_1);
            TxnRecord txnRecord = replica.txnRecords().get(txnId);

            assertNotNull(txnRecord);
            assertEquals(TxnStatus.COMMITTED, txnRecord.status());
            assertEquals(commitResponse.commitTimestamp(), txnRecord.commitTimestamp());
            assertEquals(Set.of(STORAGE_NODE_1), txnRecord.participantReplicas());

            com.tickloom.testkit.ClusterAssertions.assertEventually(cluster, () ->
                    committedValue(replica.committedStore(), "account-101", txnRecord.commitTimestamp())
                            .filter("1000"::equals)
                            .isPresent()
            );
            com.tickloom.testkit.ClusterAssertions.assertEventually(cluster, () ->
                    !intentExists(replica.intentStore(), "account-101", ts(5000)));
        }
    }

    @Test
    void txnReadCommittedValuesAtReadTimestamp() throws Exception {
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

            await(cluster, client.beginTransaction(txnId, IsolationLevel.SNAPSHOT));

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(txnId, "account-101", ts(1000))
            );

            assertTrue(readResponse.found());
            assertEquals("750", readResponse.value());
        }
    }
}
