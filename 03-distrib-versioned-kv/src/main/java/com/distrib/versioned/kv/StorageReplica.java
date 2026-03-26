package com.distrib.versioned.kv;

import clock.HybridClock;
import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StorageReplica extends Replica {
    private final MVCCStore store;
    private final HybridClock hybridClock;

    public StorageReplica(MVCCStore store, List<ProcessId> peerIds, ProcessParams processParams) {
        super(peerIds, processParams);
        this.store = store;
        this.hybridClock = new HybridClock(() -> clock.now());
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
       return Map.of(
               StorageMessageTypes.WRITE_REQUEST, this::handleWriteRequest,
               StorageMessageTypes.READ_REQUEST, this::handleReadRequest
       );
    }

    private void handleWriteRequest(Message message) {
        WriteRequest request = deserializePayload(message.payload(), WriteRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTimestamp());
        WriteResponse response = write(request, propagatedTime);
        sendResponse(message, response, StorageMessageTypes.WRITE_RESPONSE);
    }

    private void handleReadRequest(Message message) {
        ReadRequest request = deserializePayload(message.payload(), ReadRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTimestamp());
        ReadResponse response = read(request, propagatedTime);
        sendResponse(message, response, StorageMessageTypes.READ_RESPONSE);
    }

    private WriteResponse write(WriteRequest request, HybridTimestamp writeTimestamp) {
        try {
            storeVersionedValue(request.key(), request.value(), writeTimestamp);
            return WriteResponse.success(writeTimestamp);
        } catch (Exception e) {
            return WriteResponse.failure(hybridClock.now(), e.getMessage());
        }
    }

    private ReadResponse read(ReadRequest request, HybridTimestamp propagatedTime) {
        try {
            Optional<String> value = readValueAsOf(request.key(), request.readAt());
            return value.map(foundValue -> ReadResponse.found(foundValue, propagatedTime))
                    .orElseGet(() -> ReadResponse.notFound(propagatedTime));
        } catch (Exception e) {
            return ReadResponse.failure(hybridClock.now(), e.getMessage());
        }
    }

    private HybridTimestamp mergeClock(HybridTimestamp remoteTimestamp) {
        return hybridClock.tick(remoteTimestamp);
    }

    private void storeVersionedValue(String key, String value, HybridTimestamp versionTimestamp) {
        store.put(versionedKey(key, versionTimestamp), encodeValue(value));
    }

    private Optional<String> readValueAsOf(String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).map(OrderPreservingCodec::decodeString);
    }

    private MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(OrderPreservingCodec.encodeString(key), timestamp);
    }

    private byte[] encodeValue(String value) {
        return OrderPreservingCodec.encodeString(value);
    }

    private void sendResponse(Message message, Object response, MessageType responseType) {
        send(createResponseMessage(message, response, responseType));
    }
}
