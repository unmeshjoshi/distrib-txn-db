package com.distrib.txn.kv.dsl;

sealed interface ScenarioStep {

    record BeginStep(String clientName) implements ScenarioStep {}

    record WriteStep(String clientName, String key, String value,
                     WriteExpectation expectation) implements ScenarioStep {}

    record ReadStep(String clientName, String key,
                    ReadExpectation expectation, String expectedValue) implements ScenarioStep {}

    record CommitStep(String clientName) implements ScenarioStep {}

    enum WriteExpectation {
        SUCCESS, REJECTED
    }

    enum ReadExpectation {
        FOUND, NOT_FOUND
    }
}
