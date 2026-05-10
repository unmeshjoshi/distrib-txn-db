package com.distrib.txn.kv.dsl;

import com.tickloom.ProcessId;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class TopologyConstraint {

    public static NodeRef keyOwner() {
        return NodeRef.KEY_OWNER;
    }

    public static NodeRef coordinatorOf(String clientName) {
        return new NodeRef(clientName);
    }

    public static ClientConstraintBuilder where(String clientName) {
        return new ClientConstraintBuilder(clientName, new ArrayList<>());
    }

    public record NodeRef(String clientName) {
        static final NodeRef KEY_OWNER = new NodeRef("__key_owner__");

        public boolean isKeyOwner() {
            return "__key_owner__".equals(clientName);
        }
    }

    public static class ClientConstraintBuilder {
        private final String currentClient;
        private final List<Constraint> constraints;

        ClientConstraintBuilder(String clientName, List<Constraint> constraints) {
            this.currentClient = clientName;
            this.constraints = constraints;
        }

        public ClientConstraintBuilder coordinatorIsNotSameAs(NodeRef ref) {
            constraints.add(new Constraint(currentClient, ref));
            return this;
        }

        public ClientConstraintBuilder and(String clientName) {
            return new ClientConstraintBuilder(clientName, constraints);
        }

        public Predicate<TopologyScenario> toPredicate(List<String> clientNames) {
            return topology -> {
                for (Constraint c : constraints) {
                    ProcessId clientCoord = resolveCoordinator(c.clientName, clientNames, topology);
                    ProcessId otherNode = resolveNode(c.notSameAs, clientNames, topology);
                    if (clientCoord.equals(otherNode)) {
                        return false;
                    }
                }
                return true;
            };
        }

        private ProcessId resolveCoordinator(String clientName, List<String> clientNames, TopologyScenario topology) {
            int index = clientNames.indexOf(clientName);
            if (index == 0) return topology.coordinator1();
            if (index == 1) return topology.coordinator2();
            throw new IllegalArgumentException("Unknown client: " + clientName);
        }

        private ProcessId resolveNode(NodeRef ref, List<String> clientNames, TopologyScenario topology) {
            if (ref.isKeyOwner()) {
                return topology.keyOwner();
            }
            return resolveCoordinator(ref.clientName(), clientNames, topology);
        }

        List<Constraint> constraints() {
            return constraints;
        }
    }

    record Constraint(String clientName, NodeRef notSameAs) {}
}
