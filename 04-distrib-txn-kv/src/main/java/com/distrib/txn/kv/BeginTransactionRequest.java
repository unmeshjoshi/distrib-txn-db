package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record BeginTransactionRequest(
        TxnId txnId,
        IsolationLevel isolationLevel,
        HybridTimestamp clientTime) {
}
