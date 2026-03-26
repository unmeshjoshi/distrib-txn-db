package com.distrib.versioned.kv;

import clock.HybridTimestamp;

public record WriteResponse(
        boolean success,
        HybridTimestamp propagatedTime,
        String error) {
    public static WriteResponse success(HybridTimestamp propagatedTime) {
        return new WriteResponse(true, propagatedTime, null);
    }

    public static WriteResponse failure(HybridTimestamp propagatedTime, String error) {
        return new WriteResponse(false, propagatedTime, error);
    }
}
