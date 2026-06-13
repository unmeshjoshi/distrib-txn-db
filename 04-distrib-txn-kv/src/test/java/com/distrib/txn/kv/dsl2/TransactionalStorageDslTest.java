package com.distrib.txn.kv.dsl2;

import clock.HybridTimestamp;
import com.distrib.txn.kv.BeginTransactionResponse;
import com.distrib.txn.kv.CommitTransactionResponse;
import com.distrib.txn.kv.IsolationLevel;
import com.distrib.txn.kv.TransactionalStorageClient;
import com.distrib.txn.kv.TransactionalStorageReplica;
import com.distrib.txn.kv.TxnId;
import com.distrib.txn.kv.TxnWriteResponse;
import com.tickloom.ProcessId;
import com.tickloom.history.JepsenHistory;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.dsl.semanticmodel.Scenario;
import kv.MVCCKey;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TransactionalStorageDslTest {

    private static final ProcessId NODE1 = ProcessId.of("node1");
    private static final ProcessId NODE2 = ProcessId.of("node2");
    private static final ProcessId NODE3 = ProcessId.of("node3");

    private static final ProcessId CLIENT = ProcessId.of("client1");
    private static final ProcessId READER = ProcessId.of("reader");
    private static final ProcessId WRITER = ProcessId.of("writer");

    @Test
    void beginTransactionAndWriteUsingDsl() throws Exception {
        TxnId txnId = TxnId.of("txn-1");

        HybridTimestamp seedTime = new HybridTimestamp(1_000L, 0);

        Scenario<TransactionalStorageClient> s = TxnScenarios.scenario("Core Flow Test")
                .servers(NODE1, NODE2, NODE3)
                .client(CLIENT).connectedTo(NODE1)
                .given(g -> g.clientHlc(CLIENT, seedTime))
                .steps(steps -> {
                    steps.client(CLIENT).beginTransaction(txnId, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(CLIENT).writes(txnId, "account-101", "1000")
                         .expectResponse(TxnWriteResponse::success);

                    steps.client(CLIENT).reads(txnId, "account-101")
                         .expectSuccess();

                    steps.client(CLIENT).commits(txnId)
                         .expectResponse(CommitTransactionResponse::success);
                });

        JepsenHistory history = s.run();
    }

    @Test
    void firstCommitterWinsRejectsWriteFromOlderSnapshot() throws Exception {
        TxnId readerTxn = TxnId.of("txn-reader");
        TxnId writerTxn = TxnId.of("txn-writer");
        String sharedKey = "account-101";
        HybridTimestamp seed = new HybridTimestamp(1_000L, 0);

        Scenario<TransactionalStorageClient> s = TxnScenarios.scenario("SI first-committer-wins")
                .servers(NODE1, NODE2, NODE3)
                .client(READER).connectedTo(NODE1)
                .client(WRITER).connectedTo(NODE1)
                .given(g -> g.clientHlc(READER, seed).clientHlc(WRITER, seed))
                .steps(steps -> {
                    // Reader begins first so its snapshot is captured before any writes exist.
                    // HLC monotonicity then guarantees readerTxn.readTs < writerTxn.commitTs,
                    // which is the precondition the SI first-committer-wins rule needs to fire.
                    steps.client(READER).beginTransaction(readerTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(WRITER).beginTransaction(writerTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(WRITER).writes(writerTxn, sharedKey, "1000")
                         .expectResponse(TxnWriteResponse::success);

                    steps.client(WRITER).commits(writerTxn)
                         .expectResponse(CommitTransactionResponse::success);

                    // The commit response races the resolve message that moves the writer's intent
                    // into the committed store. Without this barrier the reader's write could
                    // arrive at the key owner before the committed version exists; the key owner
                    // would then see the writer's intent, run the GetTransactionStatus dance from
                    // writeItentFor (find intent -> resolve -> retry), and eventually produce the
                    // same "Conflicting committed transaction" error — but via the resolve-on-read
                    // path, not the clean SI check against an already-committed version. We want
                    // this test to demonstrate the latter, so we wait for the committed value to
                    // land before issuing the reader's losing write.
                    steps.await(cluster -> hasCommittedValue(cluster, sharedKey));

                    // First-committer-wins: writer's commit timestamp is now above reader's
                    // snapshot, so the key owner must reject this write.
                    steps.client(READER).writes(readerTxn, sharedKey, "2000")
                         .expectResponse(r -> !r.success()
                                 && "Conflicting committed transaction".equals(r.error()));
                });

        s.run();
    }

    private static boolean hasCommittedValue(Cluster cluster, String key) {
        HybridTimestamp upperBound = new HybridTimestamp(Long.MAX_VALUE, 0);
        MVCCKey lookup = new MVCCKey(OrderPreservingCodec.encodeString(key), upperBound);
        for (ProcessId node : List.of(NODE1, NODE2, NODE3)) {
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(node);
            if (replica.committedStore().getAsOf(lookup).isPresent()) {
                return true;
            }
        }
        return false;
    }
}
