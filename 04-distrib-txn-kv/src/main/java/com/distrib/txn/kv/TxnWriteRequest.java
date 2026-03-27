package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record TxnWriteRequest(
        TxnId txnId,
        String key,
        String value,
        HybridTimestamp clientTime) {
}
