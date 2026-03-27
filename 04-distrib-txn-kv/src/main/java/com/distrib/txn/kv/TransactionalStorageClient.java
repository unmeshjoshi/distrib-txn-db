package com.distrib.txn.kv;

import clock.HybridClock;
import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionalStorageClient extends ClusterClient {
    private final Map<TxnId, Map<ProcessId, Set<String>>> writesByParticipant;
    private final Map<TxnId, HybridTimestamp> transactionStartTimestamps;
    private final List<ProcessId> canonicalReplicas;
    private final HybridClock hybridClock;

    public TransactionalStorageClient(List<ProcessId> replicas, ProcessParams processParams) {
        super(replicas, processParams);
        this.writesByParticipant = new HashMap<>();
        this.transactionStartTimestamps = new HashMap<>();
        this.canonicalReplicas = ReplicaRouting.canonicalReplicaOrder(replicas);
        this.hybridClock = new HybridClock(processParams.clock());
    }

    HybridClock hybridClock() {
        return hybridClock;
    }

    public ListenableFuture<BeginTransactionResponse> beginTransaction(TxnId txnId, IsolationLevel isolationLevel) {
        ListenableFuture<BeginTransactionResponse> future = sendRequest(
                new BeginTransactionRequest(txnId, isolationLevel, hybridClock.now()),
                coordinatorFor(txnId),
                TransactionalMessageTypes.BEGIN_TRANSACTION_REQUEST
        );
        future.handle((response, error) -> {
            if (error == null && response != null && response.success()) {
                transactionStartTimestamps.put(txnId, response.propagatedTime());
            }
        });
        return future;
    }

    public ListenableFuture<TxnWriteResponse> write(TxnId txnId, String key, String value) {
        ListenableFuture<TxnWriteResponse> future = sendRequest(
                new TxnWriteRequest(txnId, key, value, hybridClock.now()),
                replicaFor(key),
                TransactionalMessageTypes.TXN_WRITE_REQUEST
        );

        ProcessId participant = replicaFor(key);
        future.handle((response, error) -> {
            if (error == null && response != null && response.success()) {
                trackWrite(txnId, participant, key);
            }
        });

        return future;
    }

    public ListenableFuture<TxnReadResponse> read(TxnId txnId, String key) {
        return read(txnId, key, transactionStartTimeFor(txnId), hybridClock.now());
    }

    public ListenableFuture<TxnReadResponse> read(
            TxnId txnId,
            String key,
            HybridTimestamp readTimestamp
    ) {
        return read(txnId, key, readTimestamp, hybridClock.now());
    }

    protected ListenableFuture<TxnReadResponse> read(
            TxnId txnId,
            String key,
            HybridTimestamp readTimestamp,
            HybridTimestamp clientTime
    ) {
        ListenableFuture<TxnReadResponse> future = sendRequest(
                new TxnReadRequest(txnId, key, readTimestamp, clientTime),
                replicaFor(key),
                TransactionalMessageTypes.TXN_READ_REQUEST
        );
        return future;
    }

    public ListenableFuture<CommitTransactionResponse> commit(TxnId txnId) {
        return commit(txnId, hybridClock.now());
    }

    protected ListenableFuture<CommitTransactionResponse> commit(
            TxnId txnId,
            HybridTimestamp clientTime
    ) {
        ListenableFuture<CommitTransactionResponse> future = sendRequest(
                new CommitTransactionRequest(txnId, participantWritesFor(txnId), clientTime),
                coordinatorFor(txnId),
                TransactionalMessageTypes.COMMIT_TRANSACTION_REQUEST
        );
        future.handle((response, error) -> {
            if (error == null && response != null && response.success()) {
                writesByParticipant.remove(txnId);
                transactionStartTimestamps.remove(txnId);
            }
        });
        return future;
    }

    ProcessId coordinatorFor(TxnId txnId) {
        return ReplicaRouting.coordinatorFor(txnId, canonicalReplicas);
    }

    ProcessId replicaFor(String key) {
        return ReplicaRouting.replicaFor(key, canonicalReplicas);
    }

    private void trackWrite(TxnId txnId, ProcessId participant, String key) {
        writesByParticipant
                .computeIfAbsent(txnId, ignored -> new HashMap<>())
                .computeIfAbsent(participant, ignored -> new HashSet<>())
                .add(key);
    }

    private List<ParticipantWrites> participantWritesFor(TxnId txnId) {
        Map<ProcessId, Set<String>> writes = writesByParticipant.get(txnId);
        if (writes == null) {
            return List.of();
        }

        List<ParticipantWrites> snapshot = new ArrayList<>();
        for (Map.Entry<ProcessId, Set<String>> entry : writes.entrySet()) {
            snapshot.add(new ParticipantWrites(entry.getKey(), Set.copyOf(entry.getValue())));
        }
        return List.copyOf(snapshot);
    }

    protected HybridTimestamp transactionStartTimeFor(TxnId txnId) {
        HybridTimestamp readTimestamp = transactionStartTimestamps.get(txnId);
        if (readTimestamp == null) {
            throw new IllegalStateException("Transaction " + txnId + " has not been started");
        }
        return readTimestamp;
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                TransactionalMessageTypes.BEGIN_TRANSACTION_RESPONSE, this::handleBeginTransactionResponse,
                TransactionalMessageTypes.TXN_WRITE_RESPONSE, this::handleTxnWriteResponse,
                TransactionalMessageTypes.TXN_READ_RESPONSE, this::handleTxnReadResponse,
                TransactionalMessageTypes.COMMIT_TRANSACTION_RESPONSE, this::handleCommitTransactionResponse
        );
    }

    private void handleBeginTransactionResponse(Message message) {
        BeginTransactionResponse response = deserialize(message.payload(), BeginTransactionResponse.class);
        hybridClock.tick(response.propagatedTime());
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleTxnWriteResponse(Message message) {
        TxnWriteResponse response = deserialize(message.payload(), TxnWriteResponse.class);
        hybridClock.tick(response.propagatedTime());
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleTxnReadResponse(Message message) {
        TxnReadResponse response = deserialize(message.payload(), TxnReadResponse.class);
        hybridClock.tick(response.propagatedTime());
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleCommitTransactionResponse(Message message) {
        CommitTransactionResponse response = deserialize(message.payload(), CommitTransactionResponse.class);
        hybridClock.tick(response.propagatedTime());
        handleResponse(message.correlationId(), response, message.source());
    }
}
