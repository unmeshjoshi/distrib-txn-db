package com.distrib.versioned.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageReplicaClusterTest {
    private static final ProcessId STORAGE_NODE_1 = ProcessId.of("storage-node-1");
    private static final ProcessId STORAGE_NODE_2 = ProcessId.of("storage-node-2");
    private static final ProcessId STORAGE_NODE_3 = ProcessId.of("storage-node-3");
    private static final ProcessId CLIENT = ProcessId.of("client");

    @Test
    void clientRoutesDifferentKeysToDifferentReplicas() throws Exception {
        List<ProcessId> storageNodes = List.of(STORAGE_NODE_1, STORAGE_NODE_2, STORAGE_NODE_3);
        Map<ProcessId, InMemoryMVCCStore> storesByReplica = new LinkedHashMap<>();
        storageNodes.forEach(processId -> storesByReplica.put(processId, new InMemoryMVCCStore()));

        try (Cluster cluster = new Cluster()
                .withProcessIds(storageNodes)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new StorageReplica(storesByReplica.get(params.id()), peerIds, params))
                .start()) {

            StorageReplicaClient client = cluster.newClient(CLIENT, StorageReplicaClient::new);
            Map<ProcessId, String> keyByReplica = keysByReplica(client, storageNodes);

            for (Map.Entry<ProcessId, String> entry : keyByReplica.entrySet()) {
                String key = entry.getValue();
                String value = "value-for-" + entry.getKey().name();

                WriteResponse writeResponse = await(cluster, client.write(key, value, ts(1000)));
                assertTrue(writeResponse.success());

                ReadResponse readResponse = await(cluster, client.read(key, writeResponse.propagatedTime(), ts(1100)));
                assertTrue(readResponse.found());
                assertEquals(value, readResponse.value());
            }

            for (Map.Entry<ProcessId, String> entry : keyByReplica.entrySet()) {
                assertEquals(Optional.of("value-for-" + entry.getKey().name()),
                        storedValue(storesByReplica.get(entry.getKey()), entry.getValue(), ts(5000)));
            }
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }

    private Map<ProcessId, String> keysByReplica(StorageReplicaClient client, List<ProcessId> replicas) {
        Map<ProcessId, String> keyByReplica = new LinkedHashMap<>();
        int candidate = 0;
        while (keyByReplica.size() < replicas.size()) {
            String key = "account-" + candidate++;
            keyByReplica.putIfAbsent(client.replicaFor(key), key);
        }
        return keyByReplica;
    }

    private Optional<String> storedValue(InMemoryMVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(new MVCCKey(OrderPreservingCodec.encodeString(key), readTimestamp))
                .map(OrderPreservingCodec::decodeString);
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }
}
