package com.distrib.txn.kv;

import clock.HybridClock;
import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.util.Timeout;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.distrib.txn.kv.TxnStatus.PENDING;

/**
 * Tickloom's Replica is used here as a convenient cluster-aware process abstraction. In this
 * workshop, a TransactionalStorageReplica represents one node in the cluster, not one of multiple
 * replicas of the same shard/data for replication.
 */
public class TransactionalStorageReplica extends Replica {
    private static final HybridTimestamp MAX_TIMESTAMP =
            new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE);

    //all other TransactionalStorageReplica nodes.
    private final List<ProcessId> allNodes;

    private final MVCCStore committedStore;
    private final MVCCStore intentStore;

    // The following state is only owned on the node that plays the
    // Coordinator role. Transaction records are kept in memory in this module. Each record
    // carries a tick-based timeout, similar to Tickloom's request timeouts, so abandoned
    // transactions can be evicted locally after enough ticks pass.
    private final Map<TxnId, TxnRecord> txnRecords;

    private final HybridClock hybridClock;

    public TransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            List<ProcessId> peerIds,
            ProcessParams processParams
    ) {
        super(peerIds, processParams);
        this.allNodes = ReplicaRouting.canonicalReplicaOrder(getAllNodes());
        this.committedStore = committedStore;
        this.intentStore = intentStore;
        this.txnRecords = new HashMap<>();
        this.hybridClock = new HybridClock(() -> clock.now());
    }

    MVCCStore committedStore() {
        return committedStore;
    }

    MVCCStore intentStore() {
        return intentStore;
    }

    Map<TxnId, TxnRecord> txnRecords() {
        return txnRecords;
    }

    HybridClock hybridClock() {
        return hybridClock;
    }

    @Override
    protected void onTick() {
        txnRecords.values().forEach(txnRecord -> txnRecord.heartbeatTimeout().tick());
        txnRecords.entrySet().removeIf(entry -> entry.getValue().heartbeatTimeout().fired());
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                TransactionalMessageTypes.BEGIN_TRANSACTION_REQUEST, this::handleBeginTransactionRequest,
                TransactionalMessageTypes.TXN_WRITE_REQUEST, this::handleTxnWriteRequest,
                TransactionalMessageTypes.TXN_READ_REQUEST, this::handleTxnReadRequest,
                TransactionalMessageTypes.COMMIT_TRANSACTION_REQUEST, this::handleCommitTransactionRequest,
                TransactionalMessageTypes.RESOLVE_TRANSACTION_REQUEST, this::handleResolveTransactionRequest,
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_REQUEST, this::handleGetTransactionStatusRequest,
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_RESPONSE, this::handleGetTransactionStatusResponse
        );
    }

    private void handleBeginTransactionRequest(Message message) {
        BeginTransactionRequest request = deserializePayload(message.payload(), BeginTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        BeginTransactionResponse response = beginTransaction(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.BEGIN_TRANSACTION_RESPONSE);
    }

    private void handleTxnWriteRequest(Message message) {
        TxnWriteRequest request = deserializePayload(message.payload(), TxnWriteRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());

        writeItentFor(message, request, propagatedTime);
    }

    private void handleTxnReadRequest(Message message) {
        TxnReadRequest request = deserializePayload(message.payload(), TxnReadRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        beginRead(message, request, propagatedTime);
    }

    private void handleCommitTransactionRequest(Message message) {
        CommitTransactionRequest request = deserializePayload(message.payload(), CommitTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        commitTransaction(message, request, propagatedTime);
    }

    private void handleResolveTransactionRequest(Message message) {
        ResolveTransactionRequest request = deserializePayload(message.payload(), ResolveTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        ResolveTransactionResponse response = resolve(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.RESOLVE_TRANSACTION_RESPONSE);
    }

    private void handleGetTransactionStatusRequest(Message message) {
        GetTransactionStatusRequest request =
                deserializePayload(message.payload(), GetTransactionStatusRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        GetTransactionStatusResponse response = getTransactionStatus(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.GET_TRANSACTION_STATUS_RESPONSE);
    }

    private void handleGetTransactionStatusResponse(Message message) {
        GetTransactionStatusResponse response =
                deserializePayload(message.payload(), GetTransactionStatusResponse.class);
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private BeginTransactionResponse beginTransaction(
            BeginTransactionRequest request,
            HybridTimestamp propagatedTime
    ) {
        try {
            // TODO: Exercise 1. Create a transaction record on the coordinator.
            //
            // txnRecords.put(request.txnId(), new TxnRecord(
            //         request.txnId(),
            //         PENDING,
            //         propagatedTime,
            //         null,
            //         new HashSet<>(),
            //         startedTimeout(request.txnId()),
            //         request.isolationLevel()
            // ));
            // return new BeginTransactionResponse(true, propagatedTime, null);
            return new BeginTransactionResponse(false, propagatedTime, "TODO: Exercise 1");
        } catch (Exception e) {
            return new BeginTransactionResponse(false, hybridClock.now(), e.getMessage());
        }
    }

    /**
     * Add a provisional record for the write request.
     * @param request
     * @param intentTimestamp
     * @return
     */
    private TxnWriteResponse writeIntent(TxnWriteRequest request, HybridTimestamp intentTimestamp) {
        // TODO: Exercise 2. Write a provisional intent into the intent store.
        //
        // try {
        //     intentStore.put(intentKey(request.key(), intentTimestamp), encodeIntentRecord(request));
        //     return new TxnWriteResponse(true, intentTimestamp, null);
        // } catch (Exception e) {
        //     return new TxnWriteResponse(false, hybridClock.now(), e.getMessage());
        // }
        return new TxnWriteResponse(false, hybridClock.now(), "TODO: Exercise 2");
    }

    private void beginRead(
            Message message,
            TxnReadRequest request,
            HybridTimestamp propagatedTime
    ) {
        try {
            // TODO: Exercise 3. Implement transactional reads.
            //
            // Transactional reads in this module mean "read from the transaction's snapshot,
            // plus this transaction's own writes." That is why we first check for an
            // intent/provisional record owned by the same transaction without comparing its intent timestamp to the read
            // timestamp carried on the request.
            //
            // This is intentionally different from a true historical "read at timestamp T"
            // operation. Own intents are written at replica-assigned write timestamps, which are
            // usually later than the transaction's original snapshot timestamp. If we enforced
            // intentTimestamp <= readTimestamp here, normal read-your-own-writes would break.
            //
            // If the API later needs explicit historical/as-of reads, that should be modeled as
            // a separate readAt/readAsOf method with different semantics from this transactional
            // read path.
            // Optional<String> ownIntentValue = findOwnIntentValue(request);
            // if (ownIntentValue.isPresent()) {
            //     sendReadResponse(message, new TxnReadResponse(ownIntentValue.get(), true, propagatedTime, null));
            //     return;
            // }
            //
            // Optional<StoredIntent> intentFromOtherTransaction =
            //         findLatestIntentFromOtherTransaction(request.key(), request.txnId());
            // if (intentFromOtherTransaction.isPresent()) {
            //     checkAndResolveIntents(
            //             message, request, propagatedTime, intentFromOtherTransaction.get());
            //     return;
            // }
            //
            // sendReadResponse(message, readCommitted(request, propagatedTime));
            sendReadFailure(message, "TODO: Exercise 3");
        } catch (Exception e) {
            sendReadFailure(message, e.getMessage());
        }
    }

    private ResolveTransactionResponse resolve(
            ResolveTransactionRequest request,
            HybridTimestamp propagatedTime
    ) {
        try {
            for (String key : request.keys()) {
                Optional<StoredIntent> storedIntent = findStoredIntent(key, request.txnId());
                if (storedIntent.isEmpty()) {
                    continue;
                }

                StoredIntent intent = storedIntent.get();
                committedStore.put(versionedKey(key, request.commitTimestamp()), encodeCommittedValue(intent));
                intentStore.delete(intent.key());
            }

            return new ResolveTransactionResponse(true, propagatedTime, null);
        } catch (Exception e) {
            return new ResolveTransactionResponse(false, hybridClock.now(), e.getMessage());
        }
    }

    private HybridTimestamp mergeClock(HybridTimestamp remoteTimestamp) {
        return hybridClock.tick(remoteTimestamp);
    }

    /**
     * We write provisional record. First check if there are any pending provisional records
     * for the same key.
     *  If there are, check the status talking to the transaction coordinator.
     *  If their transaction is commited, then resolve those moving writes to commited store.
     *  If their transaction is still pending, then consider that as a conflict.
     * If there are no pending provisional records, then write provisional record the write request.
     * @param message
     * @param request
     * @param intentTimestamp
     */
    private void writeItentFor(
            Message message,
            TxnWriteRequest request,
            HybridTimestamp intentTimestamp
    ) {
        if (failsSnapshotIsolationWriteValidation(message, request)) {
            return;
        }

        Optional<StoredIntent> intentFromOtherTransaction = findLatestIntentFromOtherTransaction(request);
        if (intentFromOtherTransaction.isEmpty()) {
            TxnWriteResponse response = writeIntent(request, intentTimestamp);
            sendResponse(message, response, TransactionalMessageTypes.TXN_WRITE_RESPONSE);
            return;
        }

        checkAndResolveIntents(
                message, request, intentTimestamp, intentFromOtherTransaction.get());
    }

    private void commitTransaction(
            Message message,
            CommitTransactionRequest request,
            HybridTimestamp propagatedTime
    ) {
        // TODO: Exercise 4. Mark the transaction committed and resolve its intents.
        //
        // TxnRecord txnRecord = txnRecords.get(request.txnId());
        // if (txnRecord == null) {
        //     sendCommitFailure(message, "Transaction not found: " + request.txnId());
        //     return;
        // }
        //
        // HybridTimestamp commitTimestamp = hybridClock.now();
        // Set<ProcessId> participantReplicas = new HashSet<>();
        // for (ParticipantWrites participantWrite : request.participantWrites()) {
        //     participantReplicas.add(participantWrite.participantReplica());
        // }
        //
        // txnRecords.put(request.txnId(), new TxnRecord(
        //         txnRecord.txnId(),
        //         TxnStatus.COMMITTED,
        //         txnRecord.readTimestamp(),
        //         commitTimestamp,
        //         participantReplicas,
        //         txnRecord.heartbeatTimeout(),
        //         txnRecord.isolationLevel()
        // ));
        //
        // // We do not wait for resolve responses in this module.
        // sendResolveRequests(
        //         request.txnId(),
        //         request.participantWrites(),
        //         commitTimestamp
        // );
        //
        // sendCommitResponse(
        //         message,
        //         new CommitTransactionResponse(true, commitTimestamp, commitTimestamp, null)
        // );
        sendCommitFailure(message, "TODO: Exercise 4");
    }

    //we
    private void sendResolveRequests(
            TxnId txnId,
            List<ParticipantWrites> participantWrites,
            HybridTimestamp commitTimestamp
    ) {
        for (ParticipantWrites participantWrite : participantWrites) {
            ResolveTransactionRequest request = new ResolveTransactionRequest(
                    txnId,
                    participantWrite.keys(),
                    commitTimestamp,
                    commitTimestamp
            );
            send(createMessage(
                    participantWrite.participantReplica(),
                    idGen.generateCorrelationId("resolve"),
                    request,
                    TransactionalMessageTypes.RESOLVE_TRANSACTION_REQUEST
            ));
        }
    }

    // A write encountering an intent from another transaction cannot decide locally whether that
    // intent still blocks progress. We ask the coordinator that owns the other transaction for its
    // status and then continue in the callback:
    // - PENDING: fail this write as a conflict
    // - COMMITTED: resolve the lingering intent into committed storage, then continue the write
    // - ABORTED: remove the stale intent, then continue the write
    private void checkAndResolveIntents(
            Message clientMessage,
            TxnWriteRequest writeRequest,
            HybridTimestamp intentTimestamp,
            StoredIntent intentFromOtherTransaction
    ) {
        String correlationId = idGen.generateCorrelationId("get-status");
        waitingList.add(correlationId, new com.tickloom.messaging.RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                GetTransactionStatusResponse statusResponse = (GetTransactionStatusResponse) response;
                //It's crucial to merge the clock.
                mergeClock(statusResponse.propagatedTime());

                if (statusResponse.error() != null) {
                    sendWriteFailure(clientMessage, statusResponse.error());
                    return;
                }

                //On a PENDING conflict we must stop here. There is a conflicting
                //transaction doing write.

                if (statusResponse.status() == PENDING) {
                    sendWriteFailure(clientMessage, "Conflicting pending transaction");
                    return;
                }

                //If following method returns false, that means
                resolveCommittedOrAbortedTxn(clientMessage, intentFromOtherTransaction, statusResponse);
                // Retry the write flow only after the blocking intent from the other transaction
                // has been resolved or removed.
                writeItentFor(clientMessage, writeRequest, intentTimestamp);
            }


            @Override
            public void onError(Exception error) {
                sendWriteFailure(clientMessage, error.getMessage());
            }
        });

        send(createMessage(
                coordinatorFor(intentFromOtherTransaction.intentRecord().txnId()),
                correlationId,
                new GetTransactionStatusRequest(
                        intentFromOtherTransaction.intentRecord().txnId(), hybridClock.now()),
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_REQUEST
        ));
    }

    // Reads follow the same coordinator-status lookup when they encounter an intent from another
    // transaction. The difference from writes is what happens after resolution:
    // - PENDING: for snapshot reads, ignore the pending intent and read committed data
    // - COMMITTED: resolve the intent and then read committed data
    // - ABORTED: delete the stale intent and then read committed data
    private void    checkAndResolveIntents(
            Message clientMessage,
            TxnReadRequest readRequest,
            HybridTimestamp propagatedTime,
            StoredIntent intentFromOtherTransaction
    ) {
        String correlationId = idGen.generateCorrelationId("get-status");
        waitingList.add(correlationId, new com.tickloom.messaging.RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                GetTransactionStatusResponse statusResponse = (GetTransactionStatusResponse) response;
                mergeClock(statusResponse.propagatedTime());
                if (statusResponse.error() != null) {
                    sendReadFailure(clientMessage, statusResponse.error());
                    return;
                }

                resolveCommittedOrAbortedTxn(
                        clientMessage, intentFromOtherTransaction, statusResponse);

                sendReadResponse(clientMessage, readCommitted(readRequest, propagatedTime));
            }

            @Override
            public void onError(Exception error) {
                sendReadFailure(clientMessage, error.getMessage());
            }
        });

        send(createMessage(
                coordinatorFor(intentFromOtherTransaction.intentRecord().txnId()),
                correlationId,
                new GetTransactionStatusRequest(
                        intentFromOtherTransaction.intentRecord().txnId(), hybridClock.now()),
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_REQUEST
        ));
    }

    private void resolveCommittedOrAbortedTxn(
            Message clientMessage,
            StoredIntent intentFromOtherTransaction,
            GetTransactionStatusResponse statusResponse
    ) {
        switch (statusResponse.status()) {
            case PENDING -> {
                // It is safe to ignore a pending intent from another transaction for this snapshot read. HLC
                // propagation ensures the coordinator observes a timestamp at least as high as
                // this read request, so if the transaction eventually commits, its commit
                // timestamp will be pushed above this read's snapshot timestamp.

            }
            case COMMITTED -> {
                if (statusResponse.commitTimestamp() == null) {
                    sendReadFailure(clientMessage, "Committed transaction is missing commit timestamp");
                    return;
                }
                resolveCommittedIntent(
                        //TODO: This is a problem. We should have any method expecting string key in this class.
                        OrderPreservingCodec.decodeString(intentFromOtherTransaction.key.getKey()), intentFromOtherTransaction, statusResponse.commitTimestamp());
            }
            case ABORTED -> {
                intentStore.delete(intentFromOtherTransaction.key());
            }
        }
    }

    private void resolveCommittedIntent(
            String key,
            StoredIntent intentFromOtherTransaction,
            HybridTimestamp commitTimestamp
    ) {
        committedStore.put(
                versionedKey(key, commitTimestamp),
                encodeCommittedValue(intentFromOtherTransaction)
        );
        intentStore.delete(intentFromOtherTransaction.key());
    }

    private MVCCKey intentKey(String key, HybridTimestamp intentTimestamp) {
        return new MVCCKey(OrderPreservingCodec.encodeString(key), intentTimestamp);
    }

    private byte[] encodeIntentRecord(TxnWriteRequest request) {
        return serializePayload(new IntentRecord(request.txnId(), request.value()));
    }

    private Optional<String> findOwnIntentValue(TxnReadRequest request) {
        Optional<StoredIntent> storedIntent = findStoredIntent(request.key(), request.txnId());
        return storedIntent.map(intent -> intent.intentRecord().value());
    }

    private Optional<StoredIntent> findLatestIntentFromOtherTransaction(TxnWriteRequest request) {
        return findLatestIntentFromOtherTransaction(request.key(), request.txnId());
    }

    private Optional<StoredIntent> findLatestIntentFromOtherTransaction(String key, TxnId txnId) {
        Optional<StoredIntent> latestIntent = findLatestIntent(key);
        if (latestIntent.isEmpty()) {
            return Optional.empty();
        }
        if (latestIntent.get().intentRecord().txnId().equals(txnId)) {
            return Optional.empty();
        }
        return latestIntent;
    }

    private Optional<StoredIntent> findLatestIntent(String key) {
        HybridTimestamp latestTimestamp = null;
        IntentRecord latestIntent = null;

        Map<HybridTimestamp, byte[]> versionsUpTo =
                intentStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);
        for (Map.Entry<HybridTimestamp, byte[]> entry : versionsUpTo.entrySet()) {
            if (latestTimestamp == null || entry.getKey().compareTo(latestTimestamp) > 0) {
                latestTimestamp = entry.getKey();
                latestIntent = deserializePayload(entry.getValue(), IntentRecord.class);
            }
        }

        if (latestTimestamp == null || latestIntent == null) {
            return Optional.empty();
        }

        return Optional.of(new StoredIntent(intentKey(key, latestTimestamp), latestIntent));
    }

    private Optional<StoredIntent> findStoredIntent(String key, TxnId txnId) {
        HybridTimestamp latestMatchingTimestamp = null;
        IntentRecord latestMatchingIntent = null;

        Map<HybridTimestamp, byte[]> versionsUpTo =
                intentStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);

        for (Map.Entry<HybridTimestamp, byte[]> entry : versionsUpTo.entrySet()) {
            IntentRecord intentRecord = deserializePayload(entry.getValue(), IntentRecord.class);
            if (!intentRecord.txnId().equals(txnId)) {
                continue;
            }

            if (latestMatchingTimestamp == null
                    || entry.getKey().compareTo(latestMatchingTimestamp) > 0) {
                latestMatchingTimestamp = entry.getKey();
                latestMatchingIntent = intentRecord;
            }
        }

        if (latestMatchingTimestamp == null || latestMatchingIntent == null) {
            return Optional.empty();
        }

        return Optional.of(new StoredIntent(
                intentKey(key, latestMatchingTimestamp),
                latestMatchingIntent
        ));
    }

    private Optional<String> readCommittedValue(TxnReadRequest request) {
        return committedStore.getAsOf(versionedKey(request.key(), request.readTimestamp()))
                .map(OrderPreservingCodec::decodeString);
    }

    protected TxnReadResponse readCommitted(TxnReadRequest request, HybridTimestamp propagatedTime) {
        Optional<String> committedValue = readCommittedValue(request);
        if (committedValue.isPresent()) {
            return new TxnReadResponse(committedValue.get(), true, propagatedTime, null);
        }
        return new TxnReadResponse(null, false, propagatedTime, null);
    }

    /**
     * Snapshot Isolation write-write validation.
     *
     * A transaction is allowed to write key {@code k} only if no other transaction has already
     * committed a newer version of {@code k} after this transaction's snapshot timestamp.
     *
     * In other words:
     * - transaction T reads from snapshot {@code readTimestamp}
     * - T later tries to write the same key
     * - if committedStore already contains a version with {@code commitTs > readTimestamp},
     *   then T is stale for that key and must fail
     *
     * This is the rule that prevents lost updates under Snapshot Isolation. We check it before
     * creating a new provisional intent, and again after resolving another transaction's intent,
     * because that resolution may reveal a committed version that is newer than the writer's
     * snapshot.
     */
    private boolean failsSnapshotIsolationWriteValidation(Message message, TxnWriteRequest request) {
        // TODO: Exercise 5. Add Snapshot Isolation write-write validation.
        //
        // if (!hasCommittedVersionAfter(request.key(), request.readTimestamp())) {
        //     return false;
        // }
        //
        // sendWriteFailure(message, "Conflicting committed transaction");
        // return true;
        return false;
    }

    private boolean hasCommittedVersionAfter(String key, HybridTimestamp readTimestamp) {
        Map<HybridTimestamp, byte[]> committedVersions =
                committedStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);

        for (HybridTimestamp committedTimestamp : committedVersions.keySet()) {
            if (committedTimestamp.compareTo(readTimestamp) > 0) {
                return true;
            }
        }
        return false;
    }

    private GetTransactionStatusResponse getTransactionStatus(
            GetTransactionStatusRequest request,
            HybridTimestamp propagatedTime
    ) {
        TxnRecord txnRecord = txnRecords.get(request.txnId());
        if (txnRecord == null) {
            return new GetTransactionStatusResponse(null, null, hybridClock.now(),
                    "Transaction not found: " + request.txnId());
        }
        return new GetTransactionStatusResponse(
                txnRecord.status(),
                txnRecord.commitTimestamp(),
                propagatedTime,
                null
        );
    }

    private MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(encodeLogicalKey(key), timestamp);
    }

    private byte[] encodeLogicalKey(String key) {
        return OrderPreservingCodec.encodeString(key);
    }

    private ProcessId coordinatorFor(TxnId txnId) {
        return ReplicaRouting.coordinatorFor(txnId, allNodes);
    }

    private Timeout startedTimeout(TxnId txnId) {
        Timeout timeout = new Timeout("txn-" + txnId, timeoutTicks);
        timeout.start();
        return timeout;
    }

    private byte[] encodeCommittedValue(StoredIntent storedIntent) {
        return OrderPreservingCodec.encodeString(storedIntent.intentRecord().value());
    }

    private void sendResponse(Message message, Object response, MessageType responseType) {
        send(createResponseMessage(message, response, responseType));
    }

    private void sendCommitResponse(Message message, CommitTransactionResponse response) {
        sendResponse(message, response, TransactionalMessageTypes.COMMIT_TRANSACTION_RESPONSE);
    }

    private void sendCommitFailure(Message message, String error) {
        sendCommitResponse(message, new CommitTransactionResponse(false, hybridClock.now(), null, error));
    }

    private void sendWriteResponse(Message message, TxnWriteResponse response) {
        sendResponse(message, response, TransactionalMessageTypes.TXN_WRITE_RESPONSE);
    }

    private void sendWriteFailure(Message message, String error) {
        sendWriteResponse(message, new TxnWriteResponse(false, hybridClock.now(), error));
    }

    private void sendReadResponse(Message message, TxnReadResponse response) {
        sendResponse(message, response, TransactionalMessageTypes.TXN_READ_RESPONSE);
    }

    private void sendReadFailure(Message message, String error) {
        sendReadResponse(message, new TxnReadResponse(null, false, hybridClock.now(), error));
    }

    private record StoredIntent(MVCCKey key, IntentRecord intentRecord) {
    }
}
