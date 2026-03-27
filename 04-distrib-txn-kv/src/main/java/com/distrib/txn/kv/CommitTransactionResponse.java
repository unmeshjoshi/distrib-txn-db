package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record CommitTransactionResponse(
        boolean success,
        HybridTimestamp propagatedTime,
        HybridTimestamp commitTimestamp,
        String error) {
}
