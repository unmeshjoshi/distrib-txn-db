package com.distrib.txn.kv.dsl2;

import com.distrib.txn.kv.TransactionalStorageClient;
import com.distrib.txn.kv.TransactionalStorageReplica;
import com.tickloom.testkit.dsl.Scenarios;
import com.tickloom.testkit.dsl.ServerStep;
import kv.InMemoryMVCCStore;

public final class TxnScenarios {

    public static ServerStep<TransactionalStorageClient, TxnActionScope, TxnSetupScope> scenario(String name) {
        return Scenarios.scenario(
                name,
                (peerIds, processParams) -> new TransactionalStorageReplica(
                        new InMemoryMVCCStore(),
                        new InMemoryMVCCStore(),
                        peerIds,
                        processParams
                ),
                TransactionalStorageClient::new,
                TxnStepBuilder::new
        );
    }
}
