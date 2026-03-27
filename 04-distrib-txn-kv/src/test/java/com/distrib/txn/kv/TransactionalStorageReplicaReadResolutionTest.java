package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TransactionalStorageReplicaReadResolutionTest extends TransactionalStorageReplicaTestSupport {
    @Test //we are testing only with single node to demonstrate interaction of intents and pending transactions
    void txnReadIgnoresIntentFromOtherPendingTransactionsAndReturnsCommittedValue() throws Exception {
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
            cluster.setTimeForProcess(CLIENT, 1000);
            await(cluster, client.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT, 1200);

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(readerTxn, "account-101", ts(1000))
            );

            assertTrue(readResponse.found());
            assertEquals("750", readResponse.value());
            assertTrue(intentExists(replica.intentStore(), "account-101", ts(5000)));
        }
    }

    @Test
    void txnReadResolvesCommittedIntentsFromOtherTransactionsBeforeReturningValue() throws Exception {
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
            cluster.setTimeForProcess(CLIENT, 1300);
            await(cluster, client.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));
            cluster.setTimeForProcess(CLIENT, 1400);

            TxnReadResponse readResponse = await(
                    cluster,
                    client.read(readerTxn, "account-101", ts(1300))
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
}
