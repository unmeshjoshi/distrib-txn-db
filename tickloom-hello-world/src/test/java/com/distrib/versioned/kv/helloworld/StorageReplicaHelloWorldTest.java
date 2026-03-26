package com.distrib.versioned.kv.helloworld;

import com.tickloom.ProcessId;
import com.tickloom.future.ListenableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tickloom.testkit.ClusterAssertions.assertEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageReplicaHelloWorldTest {
    private static final ProcessId STORAGE_NODE = ProcessId.of("storage-node");
    private static final ProcessId CLIENT = ProcessId.of("client");

    @Test
    void storageReplicaRepliesToHelloRequest() throws Exception {
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.of(STORAGE_NODE))
                .useSimulatedNetwork()
                .build((peerIds, params) -> new StorageReplica(peerIds, params))
                .start()) {

            StorageReplicaClient client =
                    cluster.newClientConnectedTo(CLIENT, STORAGE_NODE, StorageReplicaClient::new);

            HelloResponse response = await(cluster, client.sayHello("Tickloom"));

            assertEquals("Hello Tickloom from storage-node", response.message());
            assertEquals(STORAGE_NODE, response.replicaId());
        }
    }

    private <T> T await(Cluster cluster, ListenableFuture<T> future) {
        assertEventually(cluster, future::isCompleted);
        return future.getResult();
    }
}
