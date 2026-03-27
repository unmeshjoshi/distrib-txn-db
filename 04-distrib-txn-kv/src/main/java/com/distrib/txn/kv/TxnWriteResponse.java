package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record TxnWriteResponse(
        boolean success,
        HybridTimestamp propagatedTime,
        String error) {
}
