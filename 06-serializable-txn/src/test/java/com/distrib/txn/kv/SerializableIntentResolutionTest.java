package com.distrib.txn.kv;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SerializableIntentResolutionTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId CLIENT_1 = ProcessId.of("client-1");
    private static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    @Test
    void txnWriteFailsWhenIntentFromOtherTransactionIsStillPending() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new SerializableTransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client1 = cluster.newClient(CLIENT_1, TransactionalStorageClient::new);
            TransactionalStorageClient client2 = cluster.newClient(CLIENT_2, TransactionalStorageClient::new);

            TxnId txn1 = TxnId.of("txn-pending-write-1");
            cluster.setTimeForProcess(CLIENT_1, 1000);
            await(cluster, client1.beginTransaction(txn1, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT_1, 1100);
            await(cluster, client1.write(txn1, "account-101", "1000"));

            TxnId txn2 = TxnId.of("txn-pending-write-2");
            cluster.setTimeForProcess(CLIENT_2, 1200);
            await(cluster, client2.beginTransaction(txn2, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT_2, 1300);
            TxnWriteResponse writeResponse = await(cluster, client2.write(txn2, "account-101", "2000"));

            assertFalse(writeResponse.success());
            assertEquals("Conflicting pending transaction", writeResponse.error());
        }
    }

    @Test
    void txnReadFailsWhenIntentFromOtherTransactionIsStillPending() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new SerializableTransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient client1 = cluster.newClient(CLIENT_1, TransactionalStorageClient::new);
            TransactionalStorageClient client2 = cluster.newClient(CLIENT_2, TransactionalStorageClient::new);

            TxnId writerTxn = TxnId.of("txn-pending-read-1");
            cluster.setTimeForProcess(CLIENT_1, 1000);
            await(cluster, client1.beginTransaction(writerTxn, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT_1, 1100);
            await(cluster, client1.write(writerTxn, "account-101", "1000"));

            TxnId readerTxn = TxnId.of("txn-pending-read-2");
            cluster.setTimeForProcess(CLIENT_2, 1200);
            await(cluster, client2.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT_2, 1300);
            TxnReadResponse readResponse = await(cluster, client2.read(readerTxn, "account-101"));

            assertFalse(readResponse.found());
            assertEquals("Conflicting pending transaction", readResponse.error());
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

}
