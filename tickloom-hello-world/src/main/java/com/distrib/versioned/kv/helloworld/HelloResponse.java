package com.distrib.versioned.kv.helloworld;

import com.tickloom.ProcessId;

public record HelloResponse(String message, ProcessId replicaId) {
}
