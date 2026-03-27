package com.distrib.txn.kv;

import com.tickloom.ProcessId;

import java.util.Set;

public record ParticipantWrites(
        ProcessId participantReplica,
        Set<String> keys) {
}
