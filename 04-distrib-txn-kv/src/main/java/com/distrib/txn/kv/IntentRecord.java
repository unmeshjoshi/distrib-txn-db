package com.distrib.txn.kv;

public record IntentRecord(TxnId txnId, String value) {
}
