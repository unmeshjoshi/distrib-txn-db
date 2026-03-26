package com.distrib.versioned.kv;

import clock.HybridTimestamp;

public record WriteRequest(String key, String value, HybridTimestamp clientTime) {
    public HybridTimestamp clientTimestamp() {
        return clientTime;
    }
}
