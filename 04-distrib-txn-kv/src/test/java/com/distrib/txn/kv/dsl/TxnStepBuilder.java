package com.distrib.txn.kv.dsl;

import clock.HybridTimestamp;
import com.distrib.txn.kv.IsolationLevel;
import com.distrib.txn.kv.TransactionalStorageClient;
import com.distrib.txn.kv.TxnId;
import com.distrib.txn.kv.BeginTransactionResponse;
import com.distrib.txn.kv.TxnWriteResponse;
import com.distrib.txn.kv.TxnReadResponse;
import com.distrib.txn.kv.CommitTransactionResponse;
import com.tickloom.ProcessId;
import com.tickloom.history.Op;
import com.tickloom.testkit.dsl.EventOrAwaitScope;
import com.tickloom.testkit.dsl.StepBuilder;
import com.tickloom.testkit.dsl.semanticmodel.Action;
import com.tickloom.testkit.dsl.semanticmodel.ClusterEvent;

public class TxnStepBuilder
        extends StepBuilder<TransactionalStorageClient, TxnActionScope, TxnSetupScope>
        implements TxnActionScope, TxnSetupScope {

    @Override
    protected TxnActionScope actionBuilder() {
        return this;
    }

    @Override
    protected TxnSetupScope setupBuilder() {
        return this;
    }

    // ---- Action verbs ----

    @Override
    public EventOrAwaitScope<TxnActionScope, BeginTransactionResponse> beginTransaction(TxnId txnId, IsolationLevel isolationLevel) {
        ProcessId clientId = currentClientId();
        Action<TransactionalStorageClient, BeginTransactionResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, new Op("begin"), txnId.toString(),
                                () -> clients.get(clientId).beginTransaction(txnId, isolationLevel),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    @Override
    public EventOrAwaitScope<TxnActionScope, TxnWriteResponse> writes(TxnId txnId, String key, String value) {
        ProcessId clientId = currentClientId();
        Action<TransactionalStorageClient, TxnWriteResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, Op.WRITE, key + "=" + value,
                                () -> clients.get(clientId).write(txnId, key, value),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    @Override
    public EventOrAwaitScope<TxnActionScope, TxnReadResponse> reads(TxnId txnId, String key) {
        ProcessId clientId = currentClientId();
        Action<TransactionalStorageClient, TxnReadResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, Op.READ, key,
                                () -> clients.get(clientId).read(txnId, key),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    @Override
    public EventOrAwaitScope<TxnActionScope, CommitTransactionResponse> commits(TxnId txnId) {
        ProcessId clientId = currentClientId();
        Action<TransactionalStorageClient, CommitTransactionResponse> action = (clients, cluster, recorder) ->
                recorder.invoke(clientId, new Op("commit"), txnId.toString(),
                                () -> clients.get(clientId).commit(txnId),
                                response -> response.toString())
                        .thenApply(response -> response);
        return beginStep(action);
    }

    // ---- Setup verbs ----

    @Override
    public TxnSetupScope clientHlc(ProcessId clientId, HybridTimestamp timestamp) {
        addGiven(cluster -> {
            TransactionalStorageClient client = (TransactionalStorageClient) cluster.getClient(clientId);
            client.hybridClock().tick(timestamp);
            cluster.setTimeForProcess(clientId, timestamp.getWallClockTime());
        });
        return this;
    }

    @Override
    public TxnSetupScope hlcForAllNodes(HybridTimestamp timestamp) {
        throw new UnsupportedOperationException("hlcForAllNodes not implemented yet");
    }

    @Override
    public TxnSetupScope apply(ClusterEvent event) {
        addGiven(event);
        return this;
    }
}
