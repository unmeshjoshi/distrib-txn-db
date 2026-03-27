package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClockUncertaintySnapshotTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node-1");
    private static final ProcessId WRITER = ProcessId.of("writer");
    private static final ProcessId READER = ProcessId.of("reader");
    private static final long UNCERTAINTY_OFFSET = 10;

    @Test
    void snapshotReadCanMissCommittedValueInsideClockUncertaintyWindow() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params))
                .start()) {

            TransactionalStorageClient writer = cluster.newClient(WRITER, TransactionalStorageClient::new);
            TransactionalStorageClient reader = cluster.newClient(READER, TransactionalStorageClient::new);

            cluster.setTimeForProcess(STORAGE_NODE, 1005);
            cluster.setTimeForProcess(WRITER, 995);
            cluster.setTimeForProcess(READER, 1000);

            TxnId writerTxn = TxnId.of("txn-uncertainty-writer");
            await(cluster, writer.beginTransaction(writerTxn, IsolationLevel.SNAPSHOT));
            await(cluster, writer.write(writerTxn, "account-101", "1000"));

            CommitTransactionResponse commitResponse = await(cluster, writer.commit(writerTxn));

            assertTrue(commitResponse.success());
            assertNotNull(commitResponse.commitTimestamp());
            assertTrue(commitResponse.commitTimestamp().getWallClockTime() > 1000);
            assertTrue(commitResponse.commitTimestamp().getWallClockTime() <= 1000 + UNCERTAINTY_OFFSET);

            TxnId readerTxn = TxnId.of("txn-uncertainty-reader");
            await(cluster, reader.beginTransaction(readerTxn, IsolationLevel.SNAPSHOT));

            // This module demonstrates the issue only: the committed version is newer than the
            // snapshot timestamp, so the read misses it even though it lies within the reader's
            // clock-uncertainty window.
            TxnReadResponse uncertainRead =
                    await(cluster, reader.read(readerTxn, "account-101", ts(1000)));

            assertFalse(uncertainRead.found());

            TxnReadResponse laterRead =
                    await(cluster, reader.read(readerTxn, "account-101", ts(1000 + UNCERTAINTY_OFFSET + 1)));

            assertTrue(laterRead.found());
            assertTrue("1000".equals(laterRead.value()));
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
