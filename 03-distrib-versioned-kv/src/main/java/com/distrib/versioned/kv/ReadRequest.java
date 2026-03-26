package com.distrib.versioned.kv;

import clock.HybridTimestamp;

public record ReadRequest(String key, HybridTimestamp readTimestamp, HybridTimestamp clientTime) {
    public HybridTimestamp readAt() {
        return readTimestamp;
    }

    public HybridTimestamp clientTimestamp() {
        return clientTime;
    }
}
