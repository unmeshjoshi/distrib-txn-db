package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record GetTransactionStatusResponse(
        TxnStatus status,
        HybridTimestamp commitTimestamp,
        HybridTimestamp propagatedTime,
        String error) {
}
