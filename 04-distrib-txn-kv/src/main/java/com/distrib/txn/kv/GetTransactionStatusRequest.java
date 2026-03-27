package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record GetTransactionStatusRequest(
        TxnId txnId,
        HybridTimestamp clientTime) {
}
