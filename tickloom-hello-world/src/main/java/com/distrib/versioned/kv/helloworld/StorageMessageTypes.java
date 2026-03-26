package com.distrib.versioned.kv.helloworld;

import com.tickloom.messaging.MessageType;

public final class StorageMessageTypes {
    private StorageMessageTypes() {
    }

    public static final MessageType HELLO_REQUEST = new MessageType("HELLO_REQUEST");
    public static final MessageType HELLO_RESPONSE = new MessageType("HELLO_RESPONSE");
}
