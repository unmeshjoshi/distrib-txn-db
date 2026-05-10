package com.distrib.txn.kv.dsl;

import com.distrib.txn.kv.TxnId;
import com.tickloom.ProcessId;

public record TopologyScenario(
        String key,
        TxnId txnId1,
        TxnId txnId2,
        ProcessId keyOwner,
        ProcessId coordinator1,
        ProcessId coordinator2
) {
}
