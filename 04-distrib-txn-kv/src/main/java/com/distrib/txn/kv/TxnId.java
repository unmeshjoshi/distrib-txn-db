package com.distrib.txn.kv;

public record TxnId(String value) {
    public static TxnId of(String value) {
        return new TxnId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
