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

public class TransactionalStorageReplica extends Replica {
    private static final HybridTimestamp MAX_TIMESTAMP =
            new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE);

    private final List<ProcessId> canonicalReplicas;
    private final MVCCStore committedStore;
    private final MVCCStore intentStore;
    // Coordinator-owned transaction records are kept in memory in this module. Each record
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
        this.canonicalReplicas = ReplicaRouting.canonicalReplicaOrder(getAllNodes());
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
        beginWriteIntent(message, request, propagatedTime);
    }

    private void handleTxnReadRequest(Message message) {
        TxnReadRequest request = deserializePayload(message.payload(), TxnReadRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        beginRead(message, request, propagatedTime);
    }

    private void handleCommitTransactionRequest(Message message) {
        CommitTransactionRequest request = deserializePayload(message.payload(), CommitTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        beginCommit(message, request, propagatedTime);
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
            txnRecords.put(request.txnId(), new TxnRecord(
                    request.txnId(),
                    TxnStatus.PENDING,
                    null,
                    null,
                    new HashSet<>(),
                    startedTimeout(request.txnId()),
                    request.isolationLevel()
            ));
            return new BeginTransactionResponse(true, propagatedTime, null);
        } catch (Exception e) {
            return new BeginTransactionResponse(false, hybridClock.now(), e.getMessage());
        }
    }

    private TxnWriteResponse writeIntent(TxnWriteRequest request, HybridTimestamp intentTimestamp) {
        try {
            storeIntent(request, intentTimestamp);
            return new TxnWriteResponse(true, intentTimestamp, null);
        } catch (Exception e) {
            return new TxnWriteResponse(false, hybridClock.now(), e.getMessage());
        }
    }

    private void beginRead(
            Message message,
            TxnReadRequest request,
            HybridTimestamp propagatedTime
    ) {
        try {
            Optional<String> ownIntentValue = findOwnIntentValue(request);
            if (ownIntentValue.isPresent()) {
                sendReadResponse(message, new TxnReadResponse(ownIntentValue.get(), true, propagatedTime, null));
                return;
            }

            Optional<StoredIntent> foreignIntent = findLatestForeignIntent(request.key(), request.txnId());
            if (foreignIntent.isPresent()) {
                checkForeignIntentStatus(message, request, propagatedTime, foreignIntent.get());
                return;
            }

            sendReadResponse(message, readCommitted(request, propagatedTime));
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

    private void beginWriteIntent(
            Message message,
            TxnWriteRequest request,
            HybridTimestamp intentTimestamp
    ) {
        Optional<StoredIntent> foreignIntent = findLatestForeignIntent(request);
        if (foreignIntent.isEmpty()) {
            sendResponse(message, writeIntent(request, intentTimestamp), TransactionalMessageTypes.TXN_WRITE_RESPONSE);
            return;
        }

        checkForeignIntentStatus(message, request, intentTimestamp, foreignIntent.get());
    }

    private void beginCommit(
            Message message,
            CommitTransactionRequest request,
            HybridTimestamp propagatedTime
    ) {
        TxnRecord txnRecord = txnRecords.get(request.txnId());
        if (txnRecord == null) {
            sendCommitFailure(message, "Transaction not found: " + request.txnId());
            return;
        }

        HybridTimestamp commitTimestamp = hybridClock.now();
        Set<ProcessId> participantReplicas = new HashSet<>();
        for (ParticipantWrites participantWrite : request.participantWrites()) {
            participantReplicas.add(participantWrite.participantReplica());
        }

        txnRecords.put(request.txnId(), new TxnRecord(
                txnRecord.txnId(),
                TxnStatus.COMMITTED,
                txnRecord.readTimestamp(),
                commitTimestamp,
                participantReplicas,
                txnRecord.heartbeatTimeout(),
                txnRecord.isolationLevel()
        ));

        sendResolveRequests(
                request.txnId(),
                request.participantWrites(),
                commitTimestamp
        );
        sendCommitResponse(
                message,
                new CommitTransactionResponse(true, commitTimestamp, commitTimestamp, null)
        );
    }

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

    private void storeIntent(TxnWriteRequest request, HybridTimestamp intentTimestamp) {
        intentStore.put(intentKey(request.key(), intentTimestamp), encodeIntentRecord(request));
    }

    private void checkForeignIntentStatus(
            Message clientMessage,
            TxnWriteRequest writeRequest,
            HybridTimestamp intentTimestamp,
            StoredIntent foreignIntent
    ) {
        String correlationId = idGen.generateCorrelationId("get-status");
        waitingList.add(correlationId, new com.tickloom.messaging.RequestCallback<>() {
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                GetTransactionStatusResponse statusResponse = (GetTransactionStatusResponse) response;
                mergeClock(statusResponse.propagatedTime());
                if (statusResponse.error() != null) {
                    sendWriteFailure(clientMessage, statusResponse.error());
                    return;
                }

                handleForeignIntentStatus(clientMessage, writeRequest, intentTimestamp, foreignIntent, statusResponse);
            }

            @Override
            public void onError(Exception error) {
                sendWriteFailure(clientMessage, error.getMessage());
            }
        });

        send(createMessage(
                coordinatorFor(foreignIntent.intentRecord().txnId()),
                correlationId,
                new GetTransactionStatusRequest(foreignIntent.intentRecord().txnId(), hybridClock.now()),
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_REQUEST
        ));
    }

    private void checkForeignIntentStatus(
            Message clientMessage,
            TxnReadRequest readRequest,
            HybridTimestamp propagatedTime,
            StoredIntent foreignIntent
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

                handleForeignIntentStatus(clientMessage, readRequest, propagatedTime, foreignIntent, statusResponse);
            }

            @Override
            public void onError(Exception error) {
                sendReadFailure(clientMessage, error.getMessage());
            }
        });

        send(createMessage(
                coordinatorFor(foreignIntent.intentRecord().txnId()),
                correlationId,
                new GetTransactionStatusRequest(foreignIntent.intentRecord().txnId(), hybridClock.now()),
                TransactionalMessageTypes.GET_TRANSACTION_STATUS_REQUEST
        ));
    }

    private void handleForeignIntentStatus(
            Message clientMessage,
            TxnWriteRequest writeRequest,
            HybridTimestamp intentTimestamp,
            StoredIntent foreignIntent,
            GetTransactionStatusResponse statusResponse
    ) {
        switch (statusResponse.status()) {
            case PENDING -> sendWriteFailure(clientMessage, "Conflicting pending transaction");
            case COMMITTED -> {
                if (statusResponse.commitTimestamp() == null) {
                    sendWriteFailure(clientMessage, "Committed transaction is missing commit timestamp");
                    return;
                }
                resolveCommittedIntent(writeRequest.key(), foreignIntent, statusResponse.commitTimestamp());
                sendWriteResponse(clientMessage, writeIntent(writeRequest, intentTimestamp));
            }
            case ABORTED -> {
                intentStore.delete(foreignIntent.key());
                sendWriteResponse(clientMessage, writeIntent(writeRequest, intentTimestamp));
            }
        }
    }

    private void handleForeignIntentStatus(
            Message clientMessage,
            TxnReadRequest readRequest,
            HybridTimestamp propagatedTime,
            StoredIntent foreignIntent,
            GetTransactionStatusResponse statusResponse
    ) {
        switch (statusResponse.status()) {
            case PENDING -> {
                // It is safe to ignore a pending foreign intent for this snapshot read. HLC
                // propagation ensures the coordinator observes a timestamp at least as high as
                // this read request, so if the transaction eventually commits, its commit
                // timestamp will be pushed above this read's snapshot timestamp.
                sendReadResponse(clientMessage, readCommitted(readRequest, propagatedTime));
            }
            case COMMITTED -> {
                if (statusResponse.commitTimestamp() == null) {
                    sendReadFailure(clientMessage, "Committed transaction is missing commit timestamp");
                    return;
                }
                resolveCommittedIntent(readRequest.key(), foreignIntent, statusResponse.commitTimestamp());
                sendReadResponse(clientMessage, readCommitted(readRequest, propagatedTime));
            }
            case ABORTED -> {
                intentStore.delete(foreignIntent.key());
                sendReadResponse(clientMessage, readCommitted(readRequest, propagatedTime));
            }
        }
    }

    private void resolveCommittedIntent(String key, StoredIntent foreignIntent, HybridTimestamp commitTimestamp) {
        committedStore.put(versionedKey(key, commitTimestamp), encodeCommittedValue(foreignIntent));
        intentStore.delete(foreignIntent.key());
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

    private Optional<StoredIntent> findLatestForeignIntent(TxnWriteRequest request) {
        return findLatestForeignIntent(request.key(), request.txnId());
    }

    private Optional<StoredIntent> findLatestForeignIntent(String key, TxnId txnId) {
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

    private TxnReadResponse readCommitted(TxnReadRequest request, HybridTimestamp propagatedTime) {
        Optional<String> committedValue = readCommittedValue(request);
        if (committedValue.isPresent()) {
            return new TxnReadResponse(committedValue.get(), true, propagatedTime, null);
        }
        return new TxnReadResponse(null, false, propagatedTime, null);
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
        return ReplicaRouting.coordinatorFor(txnId, canonicalReplicas);
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
