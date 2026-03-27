package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.util.Timeout;

import java.util.Set;

public record TxnRecord(
        TxnId txnId,
        TxnStatus status,
        HybridTimestamp readTimestamp,
        HybridTimestamp commitTimestamp,
        Set<ProcessId> participantReplicas,
        Timeout heartbeatTimeout,
        IsolationLevel isolationLevel) {
}
