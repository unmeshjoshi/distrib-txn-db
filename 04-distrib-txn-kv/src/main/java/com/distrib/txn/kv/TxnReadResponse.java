package com.distrib.txn.kv;

import clock.HybridTimestamp;

public record TxnReadResponse(
        String value,
        boolean found,
        HybridTimestamp propagatedTime,
        String error) {
}
