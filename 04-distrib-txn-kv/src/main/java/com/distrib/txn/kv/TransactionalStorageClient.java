package com.distrib.txn.kv;

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

    public TransactionalStorageClient(List<ProcessId> replicas, ProcessParams processParams) {
        super(replicas, processParams);
        this.writesByParticipant = new HashMap<>();
    }

    public ListenableFuture<BeginTransactionResponse> beginTransaction(
            TxnId txnId,
            IsolationLevel isolationLevel,
            HybridTimestamp clientTime
    ) {
        return sendRequest(
                new BeginTransactionRequest(txnId, isolationLevel, clientTime),
                coordinatorFor(txnId),
                TransactionalMessageTypes.BEGIN_TRANSACTION_REQUEST
        );
    }

    public ListenableFuture<TxnWriteResponse> write(
            TxnId txnId,
            String key,
            String value,
            HybridTimestamp readTimestamp,
            HybridTimestamp clientTime
    ) {
        ListenableFuture<TxnWriteResponse> future = sendRequest(
                new TxnWriteRequest(txnId, key, value, readTimestamp, clientTime),
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

    public ListenableFuture<TxnReadResponse> read(
            TxnId txnId,
            String key,
            HybridTimestamp readTimestamp,
            HybridTimestamp clientTime
    ) {
        return sendRequest(
                new TxnReadRequest(txnId, key, readTimestamp, clientTime),
                replicaFor(key),
                TransactionalMessageTypes.TXN_READ_REQUEST
        );
    }

    public ListenableFuture<CommitTransactionResponse> commit(
            TxnId txnId,
            HybridTimestamp clientTime
    ) {
        return sendRequest(
                new CommitTransactionRequest(txnId, participantWritesFor(txnId), clientTime),
                coordinatorFor(txnId),
                TransactionalMessageTypes.COMMIT_TRANSACTION_REQUEST
        );
    }

    ProcessId coordinatorFor(TxnId txnId) {
        int index = Math.floorMod(txnId.toString().hashCode(), replicaEndpoints.size());
        return replicaEndpoints.get(index);
    }

    ProcessId replicaFor(String key) {
        int index = Math.floorMod(key.hashCode(), replicaEndpoints.size());
        return replicaEndpoints.get(index);
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
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleTxnWriteResponse(Message message) {
        TxnWriteResponse response = deserialize(message.payload(), TxnWriteResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleTxnReadResponse(Message message) {
        TxnReadResponse response = deserialize(message.payload(), TxnReadResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleCommitTransactionResponse(Message message) {
        CommitTransactionResponse response = deserialize(message.payload(), CommitTransactionResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}
