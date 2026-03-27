package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record BeginTransactionResponse(
        boolean success,
        HybridTimestamp propagatedTime,
        String error) {
}
