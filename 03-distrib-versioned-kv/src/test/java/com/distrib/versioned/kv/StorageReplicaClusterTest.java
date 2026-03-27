package com.distrib.versioned.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

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

        try (Cluster cluster = new Cluster()
                .withProcessIds(storageNodes)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new StorageReplica(new InMemoryMVCCStore(), peerIds, params))
                .start()) {

            StorageReplicaClient client = cluster.newClient(CLIENT, StorageReplicaClient::new);
            Map<String, String> kv =  Map.of(
                    "account-101", "1000",
                    "account-202", "2500",
                    "account-303", "500"
            );

            for (Map.Entry<String, String> entry : kv.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                WriteResponse writeResponse = await(cluster, client.write(key, value));
                assertTrue(writeResponse.success());

                HybridTimestamp readTimestamp = writeResponse.propagatedTime(); //we can read at time >= the time at which write happened.
                ReadResponse readResponse = await(cluster, client.read(key, readTimestamp));
                assertTrue(readResponse.found());
                assertEquals(value, readResponse.value());
            }

            assertTrue(client.hybridClock().now().compareTo(ts(1000)) >= 0);

            for (Map.Entry<String, String> entry : kv.entrySet()) {
                ProcessId owningReplica = client.replicaFor(entry.getKey());
                StorageReplica replica = (StorageReplica) cluster.getProcess(owningReplica);
                assertEquals(Optional.of(entry.getValue()),
                        storedValue(replica.store(), entry.getKey(), ts(5000)));
            }
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }


    private Optional<String> storedValue(MVCCStore store, String key, HybridTimestamp readTimestamp) {
        return store.getAsOf(new MVCCKey(OrderPreservingCodec.encodeString(key), readTimestamp))
                .map(OrderPreservingCodec::decodeString);
    }

    private static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }
}
