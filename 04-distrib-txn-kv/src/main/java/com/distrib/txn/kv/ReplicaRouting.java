package com.distrib.txn.kv;

import com.tickloom.ProcessId;

import java.util.Comparator;
import java.util.List;

public final class ReplicaRouting {
    private ReplicaRouting() {
    }

    public static ProcessId coordinatorFor(TxnId txnId, List<ProcessId> replicas) {
        return replicas.get(Math.floorMod(txnId.toString().hashCode(), replicas.size()));
    }

    public static ProcessId replicaFor(String key, List<ProcessId> replicas) {
        return replicas.get(Math.floorMod(key.hashCode(), replicas.size()));
    }

    public static List<ProcessId> canonicalReplicaOrder(List<ProcessId> replicas) {
        return replicas.stream()
                .sorted(Comparator.comparing(ProcessId::name))
                .toList();
    }
}
