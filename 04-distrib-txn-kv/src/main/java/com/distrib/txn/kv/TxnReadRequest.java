package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record TxnReadRequest(
        TxnId txnId,
        String key,
        HybridTimestamp readTimestamp,
        HybridTimestamp clientTime) {
}
