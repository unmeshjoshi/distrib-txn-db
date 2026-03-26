package com.distrib.versioned.kv;

import com.tickloom.messaging.MessageType;

public final class StorageMessageTypes {
    private StorageMessageTypes() {
    }

    public static final MessageType WRITE_REQUEST = new MessageType("WRITE_REQUEST");
    public static final MessageType WRITE_RESPONSE = new MessageType("WRITE_RESPONSE");
    public static final MessageType READ_REQUEST = new MessageType("READ_REQUEST");
    public static final MessageType READ_RESPONSE = new MessageType("READ_RESPONSE");
}
