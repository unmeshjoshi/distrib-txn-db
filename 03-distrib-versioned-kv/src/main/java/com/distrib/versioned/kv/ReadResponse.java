package com.distrib.versioned.kv;

import clock.HybridTimestamp;

public record ReadResponse(
        String value,
        boolean found,
        HybridTimestamp propagatedTime,
        String error) {
    public static ReadResponse found(String value, HybridTimestamp propagatedTime) {
        return new ReadResponse(value, true, propagatedTime, null);
    }

    public static ReadResponse notFound(HybridTimestamp propagatedTime) {
        return new ReadResponse(null, false, propagatedTime, null);
    }

    public static ReadResponse failure(HybridTimestamp propagatedTime, String error) {
        return new ReadResponse(null, false, propagatedTime, error);
    }
}
