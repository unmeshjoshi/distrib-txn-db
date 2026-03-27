package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record ResolveTransactionResponse(
        boolean success,
        HybridTimestamp propagatedTime,
        String error) {
}
