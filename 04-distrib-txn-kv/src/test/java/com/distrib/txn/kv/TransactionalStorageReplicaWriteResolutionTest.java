package com.distrib.txn.kv;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.*;

class TransactionalStorageReplicaWriteResolutionTest extends TransactionalStorageReplicaTestSupport {
    @Test
    void txnWriteFailsWhenIntentFromOtherTransactionIsStillPending() throws Exception {
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
            cluster.setTimeForProcess(CLIENT, 1000);
            tickUntilComplete(cluster, client.beginTransaction(txn1, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT, 1100);
            tickUntilComplete(cluster, client.write(txn1, "account-101", "1000"));

            TxnId txn2 = TxnId.of("txn-5b");
            cluster.setTimeForProcess(CLIENT, 1200);
            tickUntilComplete(cluster, client.beginTransaction(txn2, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT, 1300);
            TxnWriteResponse write2 = tickUntilComplete(cluster, client.write(txn2, "account-101", "2000"));

            assertFalse(write2.success());
            assertEquals("Conflicting pending transaction", write2.error());
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

            cluster.setTimeForProcess(CLIENT, 1000);
            tickUntilComplete(cluster, client1.beginTransaction(routingScenario.firstTxnId(), IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT, 1100);
            tickUntilComplete(cluster, client1.write(routingScenario.firstTxnId(), routingScenario.key(), "1000"));

            CommitTransactionResponse firstCommit =
                    tickUntilComplete(cluster, client1.commit(routingScenario.firstTxnId()));

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

            cluster.setTimeForProcess(CLIENT_2, 1300);
            tickUntilComplete(cluster, client2.beginTransaction(routingScenario.secondTxnId(), IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT_2, 1400);
            TxnWriteResponse secondWrite = tickUntilComplete(
                    cluster,
                    client2.write(routingScenario.secondTxnId(), routingScenario.key(), "2000")
            );

            assertTrue(secondWrite.success());
            assertEventually(cluster, () ->
                    committedValue(participantReplica.committedStore(), routingScenario.key(), ts(5000))
                            .filter("1000"::equals)
                            .isPresent());

            TxnReadResponse ownRead =
                    tickUntilComplete(cluster, client2.read(routingScenario.secondTxnId(), routingScenario.key(), ts(1300)));
            assertTrue(ownRead.found());
            assertEquals("2000", ownRead.value());
        }
    }
}
