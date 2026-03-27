package com.distrib.txn.kv;

import clock.HybridTimestamp;

import java.util.List;

public record CommitTransactionRequest(
        TxnId txnId,
        List<ParticipantWrites> participantWrites,
        HybridTimestamp clientTime) {
}
