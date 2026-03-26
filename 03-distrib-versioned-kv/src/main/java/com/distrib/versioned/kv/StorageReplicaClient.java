package com.distrib.versioned.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.List;
import java.util.Map;

public class StorageReplicaClient extends ClusterClient {

    public StorageReplicaClient(List<ProcessId> replicas, ProcessParams processParams) {
        super(replicas, processParams);
    }

    public ListenableFuture<WriteResponse> write(String key, String value, HybridTimestamp clientTime) {
        return sendRequest(new WriteRequest(key, value, clientTime), replicaFor(key),
                StorageMessageTypes.WRITE_REQUEST);
    }

    public ListenableFuture<ReadResponse> read(String key, HybridTimestamp readTimestamp, HybridTimestamp clientTime) {
        return sendRequest(
                new ReadRequest(key, readTimestamp, clientTime),
                replicaFor(key),
                StorageMessageTypes.READ_REQUEST);
    }

    ProcessId replicaFor(String key) {
        int index = Math.floorMod(key.hashCode(), replicaEndpoints.size());
        return replicaEndpoints.get(index);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                StorageMessageTypes.WRITE_RESPONSE, this::handleWriteResponse,
                StorageMessageTypes.READ_RESPONSE, this::handleReadResponse
        );
    }

    private void handleWriteResponse(Message message) {
        WriteResponse response = deserialize(message.payload(), WriteResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleReadResponse(Message message) {
        ReadResponse response = deserialize(message.payload(), ReadResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}
