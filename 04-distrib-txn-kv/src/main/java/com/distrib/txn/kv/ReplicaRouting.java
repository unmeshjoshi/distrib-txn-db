package com.distrib.txn.kv;

import com.tickloom.ProcessId;

import java.util.Comparator;
import java.util.List;

final class ReplicaRouting {
    private ReplicaRouting() {
    }

    static ProcessId coordinatorFor(TxnId txnId, List<ProcessId> replicas) {
        return replicas.get(Math.floorMod(txnId.toString().hashCode(), replicas.size()));
    }

    static ProcessId replicaFor(String key, List<ProcessId> replicas) {
        return replicas.get(Math.floorMod(key.hashCode(), replicas.size()));
    }

    static List<ProcessId> canonicalReplicaOrder(List<ProcessId> replicas) {
        return replicas.stream()
                .sorted(Comparator.comparing(ProcessId::name))
                .toList();
    }
}
