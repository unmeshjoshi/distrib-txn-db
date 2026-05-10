package com.distrib.txn.kv.dsl;

import clock.HybridTimestamp;
import com.distrib.txn.kv.*;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kv.InMemoryMVCCStore;
import kv.OrderPreservingCodec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionScenario {
    private int nodeCount = 3;
    private final List<ClientSpec> clients = new ArrayList<>();
    private TopologyConstraint.ClientConstraintBuilder topologyConstraint;
    private final List<ScenarioStep> steps = new ArrayList<>();
    private final Map<String, String> writeExpectedErrors = new HashMap<>();

    public static TransactionScenario create() {
        return new TransactionScenario();
    }

    public static HybridTimestamp hlc(long wallClock) {
        return new HybridTimestamp(wallClock, 0);
    }

    public TransactionScenario nodes(int n) {
        this.nodeCount = n;
        return this;
    }

    public TransactionScenario client(String name, HybridTimestamp startingHlc) {
        clients.add(new ClientSpec(name, startingHlc));
        return this;
    }

    public TransactionScenario topology(TopologyConstraint.ClientConstraintBuilder constraint) {
        this.topologyConstraint = constraint;
        return this;
    }

    public TransactionScenario begin(String clientName) {
        steps.add(new ScenarioStep.BeginStep(clientName));
        return this;
    }

    public WriteStepHandle write(String clientName, String key, String value) {
        return new WriteStepHandle(this, clientName, key, value);
    }

    public ReadStepHandle read(String clientName, String key) {
        return new ReadStepHandle(this, clientName, key);
    }

    public TransactionScenario commit(String clientName) {
        steps.add(new ScenarioStep.CommitStep(clientName));
        return this;
    }

    public ScenarioResult run() {
        List<String> clientNames = clients.stream().map(ClientSpec::name).toList();
        Predicate<TopologyScenario> predicate = topologyConstraint.toPredicate(clientNames);

        List<ProcessId> storageNodes = IntStream.rangeClosed(1, nodeCount)
                .mapToObj(i -> ProcessId.of("storage-node-" + i))
                .toList();

        Map<String, HybridTimestamp> snapshots = new HashMap<>();
        Map<String, HybridTimestamp> commitTimestamps = new HashMap<>();

        try (Cluster cluster = new Cluster()
                .withProcessIds(storageNodes)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        params
                ))
                .start()) {

            Map<String, TransactionalStorageClient> clientMap = new HashMap<>();
            TransactionalStorageClient firstClient = null;
            for (int i = 0; i < clients.size(); i++) {
                ClientSpec spec = clients.get(i);
                ProcessId clientProcessId = ProcessId.of("client-" + spec.name());
                TransactionalStorageClient client = cluster.newClient(clientProcessId, TransactionalStorageClient::new);
                clientMap.put(spec.name(), client);
                cluster.setTimeForProcess(clientProcessId, spec.startingHlc().getWallClockTime());
                if (i == 0) firstClient = client;
            }

            TopologyScenario topology = findTopology(firstClient, predicate);

            Map<String, TxnId> txnIds = new HashMap<>();
            txnIds.put(clientNames.get(0), topology.txnId1());
            txnIds.put(clientNames.get(1), topology.txnId2());

            String sharedKey = topology.key();

            for (ScenarioStep step : steps) {
                executeStep(step, cluster, clientMap, txnIds, sharedKey,
                        topology, snapshots, commitTimestamps);
            }

            Map<ProcessId, TransactionalStorageReplica> replicas = new HashMap<>();
            Map<ProcessId, HybridTimestamp> nodeHlcSnapshots = new HashMap<>();
            for (ProcessId nodeId : storageNodes) {
                TransactionalStorageReplica replica =
                        (TransactionalStorageReplica) cluster.getProcess(nodeId);
                replicas.put(nodeId, replica);
                nodeHlcSnapshots.put(nodeId, replica.hybridClock().now());
            }

            return new ScenarioResult(snapshots, commitTimestamps, nodeHlcSnapshots,
                    replicas, clientNames, topology);

        } catch (Exception e) {
            throw new RuntimeException("Scenario execution failed", e);
        }
    }

    private void executeStep(
            ScenarioStep step,
            Cluster cluster,
            Map<String, TransactionalStorageClient> clientMap,
            Map<String, TxnId> txnIds,
            String sharedKey,
            TopologyScenario topology,
            Map<String, HybridTimestamp> snapshots,
            Map<String, HybridTimestamp> commitTimestamps
    ) {
        switch (step) {
            case ScenarioStep.BeginStep s -> {
                TransactionalStorageClient client = clientMap.get(s.clientName());
                TxnId txnId = txnIds.get(s.clientName());
                BeginTransactionResponse response = cluster.tickUntilComplete(
                        client.beginTransaction(txnId, IsolationLevel.SNAPSHOT));
                assertTrue(response.success(),
                        s.clientName() + " beginTransaction failed: " + response.error());
                snapshots.put(s.clientName(), response.propagatedTime());
            }
            case ScenarioStep.WriteStep s -> {
                TransactionalStorageClient client = clientMap.get(s.clientName());
                TxnId txnId = txnIds.get(s.clientName());
                String key = resolveKey(s.key(), sharedKey);
                TxnWriteResponse response = cluster.tickUntilComplete(
                        client.write(txnId, key, s.value()));

                if (s.expectation() == ScenarioStep.WriteExpectation.REJECTED) {
                    assertFalse(response.success(),
                            s.clientName() + " write should have been rejected but succeeded");
                    String expectedError = writeExpectedErrors.get(s.clientName() + ":" + s.key());
                    if (expectedError != null) {
                        assertEquals(expectedError, response.error());
                    }
                } else {
                    assertTrue(response.success(),
                            s.clientName() + " write failed: " + response.error());
                }
            }
            case ScenarioStep.ReadStep s -> {
                TransactionalStorageClient client = clientMap.get(s.clientName());
                TxnId txnId = txnIds.get(s.clientName());
                String key = resolveKey(s.key(), sharedKey);
                TxnReadResponse response = cluster.tickUntilComplete(
                        client.read(txnId, key));

                if (s.expectation() == ScenarioStep.ReadExpectation.NOT_FOUND) {
                    assertFalse(response.found(),
                            s.clientName() + " read should not have found a value but got: " + response.value());
                } else {
                    assertTrue(response.found(),
                            s.clientName() + " read expected to find a value but didn't");
                    if (s.expectedValue() != null) {
                        assertEquals(s.expectedValue(), response.value(),
                                s.clientName() + " read returned unexpected value");
                    }
                }
            }
            case ScenarioStep.CommitStep s -> {
                TransactionalStorageClient client = clientMap.get(s.clientName());
                TxnId txnId = txnIds.get(s.clientName());
                CommitTransactionResponse response = cluster.tickUntilComplete(
                        client.commit(txnId));
                assertTrue(response.success(),
                        s.clientName() + " commit failed: " + response.error());
                commitTimestamps.put(s.clientName(), response.commitTimestamp());

                waitForResolution(cluster, topology, sharedKey);
            }
        }
    }

    private void waitForResolution(Cluster cluster, TopologyScenario topology, String sharedKey) {
        TransactionalStorageReplica keyOwner =
                (TransactionalStorageReplica) cluster.getProcess(topology.keyOwner());
        cluster.tickUntil(() ->
                keyOwner.committedStore().getAsOf(
                        new kv.MVCCKey(
                                OrderPreservingCodec.encodeString(sharedKey),
                                new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE))
                ).isPresent());
    }

    private String resolveKey(String key, String sharedKey) {
        return sharedKey;
    }

    void addStep(ScenarioStep step) {
        steps.add(step);
    }

    void addWriteExpectedError(String clientName, String key, String error) {
        writeExpectedErrors.put(clientName + ":" + key, error);
    }

    private static TopologyScenario findTopology(
            TransactionalStorageClient client,
            Predicate<TopologyScenario> constraint
    ) {
        List<String> candidateKeys = IntStream.range(0, 20)
                .mapToObj(i -> "key-" + i)
                .toList();
        List<TxnId> candidateTxnIds = IntStream.range(0, 20)
                .mapToObj(i -> TxnId.of("txn-" + i))
                .toList();

        for (String key : candidateKeys) {
            ProcessId keyOwner = client.replicaFor(key);
            for (TxnId txn1 : candidateTxnIds) {
                ProcessId coord1 = client.coordinatorFor(txn1);
                for (TxnId txn2 : candidateTxnIds) {
                    if (txn2.equals(txn1)) continue;
                    ProcessId coord2 = client.coordinatorFor(txn2);
                    TopologyScenario scenario = new TopologyScenario(
                            key, txn1, txn2, keyOwner, coord1, coord2);
                    if (constraint.test(scenario)) return scenario;
                }
            }
        }

        fail("Could not find a topology matching the required constraint.");
        throw new IllegalStateException("Unreachable");
    }

    record ClientSpec(String name, HybridTimestamp startingHlc) {}

    public static class WriteStepHandle {
        private final TransactionScenario scenario;
        private final String clientName;
        private final String key;
        private final String value;

        WriteStepHandle(TransactionScenario scenario, String clientName, String key, String value) {
            this.scenario = scenario;
            this.clientName = clientName;
            this.key = key;
            this.value = value;
        }

        public TransactionScenario expectSuccess() {
            scenario.addStep(new ScenarioStep.WriteStep(
                    clientName, key, value, ScenarioStep.WriteExpectation.SUCCESS));
            return scenario;
        }

        public TransactionScenario expectRejected() {
            scenario.addStep(new ScenarioStep.WriteStep(
                    clientName, key, value, ScenarioStep.WriteExpectation.REJECTED));
            return scenario;
        }

        public TransactionScenario expectRejected(String errorMessage) {
            scenario.addWriteExpectedError(clientName, key, errorMessage);
            scenario.addStep(new ScenarioStep.WriteStep(
                    clientName, key, value, ScenarioStep.WriteExpectation.REJECTED));
            return scenario;
        }
    }

    public static class ReadStepHandle {
        private final TransactionScenario scenario;
        private final String clientName;
        private final String key;

        ReadStepHandle(TransactionScenario scenario, String clientName, String key) {
            this.scenario = scenario;
            this.clientName = clientName;
            this.key = key;
        }

        public TransactionScenario expectNotFound() {
            scenario.addStep(new ScenarioStep.ReadStep(
                    clientName, key, ScenarioStep.ReadExpectation.NOT_FOUND, null));
            return scenario;
        }

        public TransactionScenario expectFound() {
            scenario.addStep(new ScenarioStep.ReadStep(
                    clientName, key, ScenarioStep.ReadExpectation.FOUND, null));
            return scenario;
        }

        public TransactionScenario expectValue(String expectedValue) {
            scenario.addStep(new ScenarioStep.ReadStep(
                    clientName, key, ScenarioStep.ReadExpectation.FOUND, expectedValue));
            return scenario;
        }
    }
}
