package com.distrib.txn.kv;

import com.tickloom.messaging.MessageType;

public final class TransactionalMessageTypes {
    private TransactionalMessageTypes() {
    }

    public static final MessageType BEGIN_TRANSACTION_REQUEST =
            new MessageType("BEGIN_TRANSACTION_REQUEST");
    public static final MessageType BEGIN_TRANSACTION_RESPONSE =
            new MessageType("BEGIN_TRANSACTION_RESPONSE");
    public static final MessageType TXN_WRITE_REQUEST =
            new MessageType("TXN_WRITE_REQUEST");
    public static final MessageType TXN_WRITE_RESPONSE =
            new MessageType("TXN_WRITE_RESPONSE");
    public static final MessageType TXN_READ_REQUEST =
            new MessageType("TXN_READ_REQUEST");
    public static final MessageType TXN_READ_RESPONSE =
            new MessageType("TXN_READ_RESPONSE");
    public static final MessageType COMMIT_TRANSACTION_REQUEST =
            new MessageType("COMMIT_TRANSACTION_REQUEST");
    public static final MessageType COMMIT_TRANSACTION_RESPONSE =
            new MessageType("COMMIT_TRANSACTION_RESPONSE");
    public static final MessageType RESOLVE_TRANSACTION_REQUEST =
            new MessageType("RESOLVE_TRANSACTION_REQUEST");
    public static final MessageType RESOLVE_TRANSACTION_RESPONSE =
            new MessageType("RESOLVE_TRANSACTION_RESPONSE");
    public static final MessageType GET_TRANSACTION_STATUS_REQUEST =
            new MessageType("GET_TRANSACTION_STATUS_REQUEST");
    public static final MessageType GET_TRANSACTION_STATUS_RESPONSE =
            new MessageType("GET_TRANSACTION_STATUS_RESPONSE");
}
