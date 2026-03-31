package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tickloom.ProcessId;
import com.tickloom.util.Timeout;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;

abstract class TransactionalStorageReplicaTestSupport {
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final ProcessId STORAGE_NODE_1 = ProcessId.of("storage-node-1");
    protected static final ProcessId STORAGE_NODE_2 = ProcessId.of("storage-node-2");
    protected static final ProcessId STORAGE_NODE_3 = ProcessId.of("storage-node-3");
    protected static final ProcessId CLIENT = ProcessId.of("client");
    protected static final ProcessId CLIENT_2 = ProcessId.of("client-2");

    protected Map<HybridTimestamp, byte[]> getAllCommittedVersions(MVCCStore store, String key) {
        return store.getVersionsUpTo(OrderPreservingCodec.encodeString(key), ts(Long.MAX_VALUE));
    }


    protected Optional<String> committedValue(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).map(OrderPreservingCodec::decodeString);
    }

    protected boolean intentExists(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(versionedKey(key, readTimestamp)).isPresent();
    }

    protected static MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(OrderPreservingCodec.encodeString(key), timestamp);
    }

    protected static byte[] encodeValue(String value) {
        return OrderPreservingCodec.encodeString(value);
    }

    protected static byte[] encodeIntentRecord(IntentRecord intentRecord) throws Exception {
        return OBJECT_MAPPER.writeValueAsBytes(intentRecord);
    }

    protected static Timeout startedTimeout(TxnId txnId) {
        Timeout timeout = new Timeout("txn-" + txnId, 10000);
        timeout.start();
        return timeout;
    }

    protected RoutingScenario routingScenario(TransactionalStorageClient client) {
        List<TxnId> txnIds = List.of(
                TxnId.of("txn-network-1"),
                TxnId.of("txn-network-2"),
                TxnId.of("txn-network-3"),
                TxnId.of("txn-network-4")
        );
        List<String> keys = List.of(
                "account-101",
                "account-202",
                "account-303",
                "account-404",
                "account-505"
        );

        for (TxnId firstTxnId : txnIds) {
            ProcessId coordinator = client.coordinatorFor(firstTxnId);
            for (String key : keys) {
                ProcessId participant = client.replicaFor(key);
                if (participant.equals(coordinator)) {
                    continue;
                }

                TxnId secondTxnId = TxnId.of(firstTxnId + "-writer-2");
                return new RoutingScenario(firstTxnId, secondTxnId, key, coordinator, participant);
            }
        }

        fail("Could not find a key and transaction id that route to different replicas.");
        throw new IllegalStateException("Unreachable");
    }

    protected static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }

    protected static HybridTimestamp ts(long wallClockTime, int ticks) {
        return new HybridTimestamp(wallClockTime, ticks);
    }

    protected record RoutingScenario(
            TxnId firstTxnId,
            TxnId secondTxnId,
            String key,
            ProcessId coordinator,
            ProcessId participant
    ) {
    }
}
