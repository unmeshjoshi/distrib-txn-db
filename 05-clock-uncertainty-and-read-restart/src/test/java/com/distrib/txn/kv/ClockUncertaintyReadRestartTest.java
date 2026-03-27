package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClockUncertaintyReadRestartTest {
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
            await(cluster, writer.beginTransaction(writerTxn, IsolationLevel.SNAPSHOT, ts(995)));
            await(cluster, writer.write(writerTxn, "account-101", "1000", ts(995), ts(995)));
            await(cluster, writer.commit(writerTxn, ts(995)));

            TxnId readerTxn = TxnId.of("txn-uncertainty-reader");
            await(cluster, reader.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT, ts(1000)));

            TxnReadResponse readResponse =
                    await(cluster, reader.read(readerTxn, "account-101", ts(1000), ts(1000)));

            assertTrue(readResponse.found());
            assertEquals("1000", readResponse.value());
            assertTrue(readResponse.propagatedTime().compareTo(ts(1000)) > 0);
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }
}
