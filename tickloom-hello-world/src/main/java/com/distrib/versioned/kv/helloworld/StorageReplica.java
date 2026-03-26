package com.distrib.versioned.kv.helloworld;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.List;
import java.util.Map;

public class StorageReplica extends Replica {

    public StorageReplica(List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                StorageMessageTypes.HELLO_REQUEST, this::handleHelloRequest
        );
    }

    private void handleHelloRequest(Message message) {
        HelloRequest request = deserializePayload(message.payload(), HelloRequest.class);
        HelloResponse response = new HelloResponse(
                "Hello " + request.name() + " from " + id.name(),
                id
        );
        send(createResponseMessage(message, response, StorageMessageTypes.HELLO_RESPONSE));
    }
}
