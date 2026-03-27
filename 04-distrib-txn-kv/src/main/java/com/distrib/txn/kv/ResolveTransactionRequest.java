package com.distrib.txn.kv;

import clock.HybridTimestamp;

import java.util.Set;

public record ResolveTransactionRequest(
        TxnId txnId,
        Set<String> keys,
        HybridTimestamp commitTimestamp,
        HybridTimestamp clientTime) {
}
