package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Set;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClockUncertaintyReadRestartTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId WRITER = ProcessId.of("writer");
    private static final ProcessId READER = ProcessId.of("reader");

    @Test
    void readRestartsAndReturnsCommittedValueFromInsideUncertaintyWindow() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new ClockUncertaintyTransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params
                ))
                .start()) {

            ClockUncertaintyTransactionalStorageClient writer =
                    cluster.newClient(WRITER, ClockUncertaintyTransactionalStorageClient::new);
            ClockUncertaintyTransactionalStorageClient reader =
                    cluster.newClient(READER, ClockUncertaintyTransactionalStorageClient::new);

            cluster.setTimeForProcess(STORAGE_NODE, 1005);
            cluster.setTimeForProcess(WRITER, 995);
            cluster.setTimeForProcess(READER, 1000);

            TxnId writerTxn = TxnId.of("txn-uncertainty-writer");
            await(cluster, writer.beginTransaction(writerTxn, IsolationLevel.SNAPSHOT));
            await(cluster, writer.write(writerTxn, "account-101", "1000"));
            await(cluster, writer.commit(writerTxn));

            TxnId readerTxn = TxnId.of("txn-uncertainty-reader");
            await(cluster, reader.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));

            TxnReadResponse readResponse =
                    await(cluster, reader.read(readerTxn, "account-101", ts(1000)));

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertTrue(readResponse.propagatedTime().compareTo(ts(1000)) > 0);
        }
    }

    @Test
    void readResolvesCommittedIntentAndThenRestartsInsideUncertaintyWindow() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new ClockUncertaintyTransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params
                ))
                .start()) {

            ClockUncertaintyTransactionalStorageClient reader =
                    cluster.newClient(READER, ClockUncertaintyTransactionalStorageClient::new);
            ClockUncertaintyTransactionalStorageReplica replica =
                    (ClockUncertaintyTransactionalStorageReplica) cluster.getProcess(STORAGE_NODE);

            cluster.setTimeForProcess(STORAGE_NODE, 1005);
            cluster.setTimeForProcess(READER, 1000);

            TxnId committedTxn = TxnId.of("txn-committed-foreign");
            HybridTimestamp commitTimestamp = ts(1005);

            replica.txnRecords().put(committedTxn, new TxnRecord(
                    committedTxn,
                    TxnStatus.COMMITTED,
                    ts(995),
                    commitTimestamp,
                    Set.of(STORAGE_NODE),
                    startedTimeout(committedTxn),
                    IsolationLevel.SNAPSHOT
            ));
            replica.intentStore().put(
                    versionedKey("account-101", ts(995)),
                    encodeIntentRecord(new IntentRecord(committedTxn, "1000"))
            );

            TxnId readerTxn = TxnId.of("txn-uncertainty-reader-after-resolve");
            await(cluster, reader.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));

            TxnReadResponse readResponse =
                    await(cluster, reader.read(readerTxn, "account-101", ts(1000)));

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertTrue(readResponse.propagatedTime().compareTo(ts(1000)) > 0);
            assertEquals("1000",
                    replica.committedStore().getAsOf(versionedKey("account-101", ts(5000)))
                            .map(OrderPreservingCodec::decodeString)
                            .orElseThrow());
            assertFalse(replica.intentStore().getAsOf(versionedKey("account-101", ts(5000))).isPresent());
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

    private static MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(OrderPreservingCodec.encodeString(key), timestamp);
    }

    private static byte[] encodeIntentRecord(IntentRecord intentRecord) throws Exception {
        return OBJECT_MAPPER.writeValueAsBytes(intentRecord);
    }

    private static com.tickloom.util.Timeout startedTimeout(TxnId txnId) {
        com.tickloom.util.Timeout timeout = new com.tickloom.util.Timeout("txn-" + txnId, 10000);
        timeout.start();
        return timeout;
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }
}
