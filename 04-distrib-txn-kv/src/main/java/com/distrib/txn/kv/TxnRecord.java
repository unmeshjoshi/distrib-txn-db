package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;

import java.util.Set;

public record TxnRecord(
        TxnId txnId,
        TxnStatus status,
        HybridTimestamp readTimestamp,
        HybridTimestamp commitTimestamp,
        Set<ProcessId> participantReplicas,
        HybridTimestamp lastHeartbeat,
        IsolationLevel isolationLevel) {
}
