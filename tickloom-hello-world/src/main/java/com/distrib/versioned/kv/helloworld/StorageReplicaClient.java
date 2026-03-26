package com.distrib.versioned.kv.helloworld;

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

    public ListenableFuture<HelloResponse> sayHello(String name) {
        return sendRequest(new HelloRequest(name), replicaEndpoints.getFirst(), StorageMessageTypes.HELLO_REQUEST);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                StorageMessageTypes.HELLO_RESPONSE, this::handleHelloResponse
        );
    }

    private void handleHelloResponse(Message message) {
        HelloResponse response = deserialize(message.payload(), HelloResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}
