package com.distrib.txn.kv;

import clock.HybridClock;
import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.util.Timeout;
import kv.InMemoryMVCCStore;
import kv.MVCCKey;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tickloom's Replica is used here as a convenient cluster-aware process abstraction. In this
 * workshop, a SerializableTransactionalStorageReplica represents one node in the cluster, not one
 * of multiple replicas of the same shard/data for replication.
 */
public class SerializableTransactionalStorageReplica extends Replica {
    private static final HybridTimestamp MAX_TIMESTAMP =
            new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE);

    private final List<ProcessId> canonicalReplicas;
    private final MVCCStore committedStore;
    private final MVCCStore intentStore;
    private final MVCCStore readProvisionalStore;
    private final Map<TxnId, TxnRecord> txnRecords;
    private final HybridClock hybridClock;

    public SerializableTransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            List<ProcessId> peerIds,
            ProcessParams processParams
    ) {
        this(committedStore, intentStore, new InMemoryMVCCStore(), peerIds, processParams);
    }

    public SerializableTransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            MVCCStore readProvisionalStore,
            List<ProcessId> peerIds,
            ProcessParams processParams
    ) {
        super(peerIds, processParams);
        this.canonicalReplicas = ReplicaRouting.canonicalReplicaOrder(getAllNodes());
        this.committedStore = committedStore;
        this.intentStore = intentStore;
        this.readProvisionalStore = readProvisionalStore;
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
        HybridTimestamp intentTimestamp = mergeClock(request.clientTime());
        beginWriteIntent(message, request, intentTimestamp);
    }

    private void handleTxnReadRequest(Message message) {
        TxnReadRequest request = deserializePayload(message.payload(), TxnReadRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        beginRead(message, request, propagatedTime);
    }

    private void handleCommitTransactionRequest(Message message) {
        CommitTransactionRequest request = deserializePayload(message.payload(), CommitTransactionRequest.class);
        mergeClock(request.clientTime());

        TxnRecord txnRecord = txnRecords.get(request.txnId());
        if (txnRecord == null) {
            sendResponse(
                    message,
                    new CommitTransactionResponse(false, hybridClock.now(), null,
                            "Transaction not found: " + request.txnId()),
                    TransactionalMessageTypes.COMMIT_TRANSACTION_RESPONSE
            );
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

        sendResolveRequests(request.txnId(), request.participantWrites(), commitTimestamp);
        sendResponse(
                message,
                new CommitTransactionResponse(true, commitTimestamp, commitTimestamp, null),
                TransactionalMessageTypes.COMMIT_TRANSACTION_RESPONSE
        );
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
            intentStore.put(intentKey(request.key(), intentTimestamp), encodeIntentRecord(request));
            return new TxnWriteResponse(true, intentTimestamp, null);
        } catch (Exception e) {
            return new TxnWriteResponse(false, hybridClock.now(), e.getMessage());
        }
    }

    private void beginWriteIntent(
            Message message,
            TxnWriteRequest request,
            HybridTimestamp intentTimestamp
    ) {
        Optional<StoredIntent> intentFromOtherTransaction =
                findLatestIntentFromOtherTransaction(request.key(), request.txnId());
        if (intentFromOtherTransaction.isPresent()) {
            checkIntentFromOtherTransactionStatus(
                    message, request, intentTimestamp, intentFromOtherTransaction.get());
            return;
        }

        completeWrite(message, request, intentTimestamp);
    }

    private void completeWrite(
            Message message,
            TxnWriteRequest request,
            HybridTimestamp intentTimestamp
    ) {
        Optional<TxnId> conflictingTxn = conflictingPendingReadTxn(request.key(), request.txnId());
        if (conflictingTxn.isPresent() && losesTieBreak(request.txnId(), conflictingTxn.get())) {
            sendResponse(
                    message,
                    new TxnWriteResponse(false, hybridClock.now(), "Conflicting serializable read"),
                    TransactionalMessageTypes.TXN_WRITE_RESPONSE
            );
            return;
        }

        sendResponse(message, writeIntent(request, intentTimestamp), TransactionalMessageTypes.TXN_WRITE_RESPONSE);
    }

    private void beginRead(
            Message message,
            TxnReadRequest request,
            HybridTimestamp propagatedTime
    ) {
        try {
            Optional<String> ownIntentValue = findOwnIntentValue(request.key(), request.txnId());
            if (ownIntentValue.isPresent()) {
                sendResponse(
                        message,
                        new TxnReadResponse(ownIntentValue.get(), true, propagatedTime, null),
                        TransactionalMessageTypes.TXN_READ_RESPONSE
                );
                return;
            }

            Optional<StoredIntent> intentFromOtherTransaction =
                    findLatestIntentFromOtherTransaction(request.key(), request.txnId());
            if (intentFromOtherTransaction.isPresent()) {
                checkIntentFromOtherTransactionStatus(
                        message, request, propagatedTime, intentFromOtherTransaction.get());
                return;
            }

            sendResponse(message, readCommittedAndRecordProvisional(request, propagatedTime),
                    TransactionalMessageTypes.TXN_READ_RESPONSE);
        } catch (Exception e) {
            sendResponse(
                    message,
                    new TxnReadResponse(null, false, hybridClock.now(), e.getMessage()),
                    TransactionalMessageTypes.TXN_READ_RESPONSE
            );
        }
    }

    private TxnReadResponse readCommittedAndRecordProvisional(
            TxnReadRequest request,
            HybridTimestamp propagatedTime
    ) {
        Optional<String> committedValue = committedStore
                .getAsOf(versionedKey(request.key(), request.readTimestamp()))
                .map(OrderPreservingCodec::decodeString);

        recordReadProvisional(request, propagatedTime);

        if (committedValue.isPresent()) {
            return new TxnReadResponse(committedValue.get(), true, propagatedTime, null);
        }
        return new TxnReadResponse(null, false, propagatedTime, null);
    }

    private void recordReadProvisional(TxnReadRequest request, HybridTimestamp readMarkerTimestamp) {
        readProvisionalStore.put(
                versionedKey(request.key(), readMarkerTimestamp),
                serializePayload(new ReadProvisionalRecord(request.txnId()))
        );
    }

    private Optional<TxnId> conflictingPendingReadTxn(String key, TxnId writerTxnId) {
        Map<HybridTimestamp, byte[]> readMarkers =
                readProvisionalStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);

        TxnId conflictingTxn = null;
        for (Map.Entry<HybridTimestamp, byte[]> marker : readMarkers.entrySet()) {
            ReadProvisionalRecord readRecord =
                    deserializePayload(marker.getValue(), ReadProvisionalRecord.class);
            if (readRecord.txnId().equals(writerTxnId)) {
                continue;
            }

            TxnRecord txnRecord = txnRecords.get(readRecord.txnId());
            if (txnRecord == null || txnRecord.status() != TxnStatus.PENDING) {
                readProvisionalStore.delete(versionedKey(key, marker.getKey()));
                continue;
            }

            if (conflictingTxn == null || readRecord.txnId().toString().compareTo(conflictingTxn.toString()) < 0) {
                conflictingTxn = readRecord.txnId();
            }
        }

        return Optional.ofNullable(conflictingTxn);
    }

    private void checkIntentFromOtherTransactionStatus(
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
                mergeClock(statusResponse.propagatedTime());
                if (statusResponse.error() != null) {
                    sendResponse(
                            clientMessage,
                            new TxnWriteResponse(false, hybridClock.now(), statusResponse.error()),
                            TransactionalMessageTypes.TXN_WRITE_RESPONSE
                    );
                    return;
                }

                handleIntentFromOtherTransactionStatus(
                        clientMessage, writeRequest, intentTimestamp, intentFromOtherTransaction, statusResponse);
            }

            @Override
            public void onError(Exception error) {
                sendResponse(
                        clientMessage,
                        new TxnWriteResponse(false, hybridClock.now(), error.getMessage()),
                        TransactionalMessageTypes.TXN_WRITE_RESPONSE
                );
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

    private void checkIntentFromOtherTransactionStatus(
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
                    sendResponse(
                            clientMessage,
                            new TxnReadResponse(null, false, hybridClock.now(), statusResponse.error()),
                            TransactionalMessageTypes.TXN_READ_RESPONSE
                    );
                    return;
                }

                handleIntentFromOtherTransactionStatus(
                        clientMessage, readRequest, propagatedTime, intentFromOtherTransaction, statusResponse);
            }

            @Override
            public void onError(Exception error) {
                sendResponse(
                        clientMessage,
                        new TxnReadResponse(null, false, hybridClock.now(), error.getMessage()),
                        TransactionalMessageTypes.TXN_READ_RESPONSE
                );
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

    private void handleIntentFromOtherTransactionStatus(
            Message clientMessage,
            TxnWriteRequest writeRequest,
            HybridTimestamp intentTimestamp,
            StoredIntent intentFromOtherTransaction,
            GetTransactionStatusResponse statusResponse
    ) {
        switch (statusResponse.status()) {
            case PENDING -> sendResponse(
                    clientMessage,
                    new TxnWriteResponse(false, hybridClock.now(), "Conflicting pending transaction"),
                    TransactionalMessageTypes.TXN_WRITE_RESPONSE
            );
            case COMMITTED -> {
                if (statusResponse.commitTimestamp() == null) {
                    sendResponse(
                            clientMessage,
                            new TxnWriteResponse(false, hybridClock.now(),
                                    "Committed transaction is missing commit timestamp"),
                            TransactionalMessageTypes.TXN_WRITE_RESPONSE
                    );
                    return;
                }
                resolveCommittedIntent(
                        writeRequest.key(), intentFromOtherTransaction, statusResponse.commitTimestamp());
                completeWrite(clientMessage, writeRequest, intentTimestamp);
            }
            case ABORTED -> {
                intentStore.delete(intentFromOtherTransaction.key());
                completeWrite(clientMessage, writeRequest, intentTimestamp);
            }
        }
    }

    private void handleIntentFromOtherTransactionStatus(
            Message clientMessage,
            TxnReadRequest readRequest,
            HybridTimestamp propagatedTime,
            StoredIntent intentFromOtherTransaction,
            GetTransactionStatusResponse statusResponse
    ) {
        switch (statusResponse.status()) {
            case PENDING -> sendResponse(
                    clientMessage,
                    new TxnReadResponse(null, false, hybridClock.now(), "Conflicting pending transaction"),
                    TransactionalMessageTypes.TXN_READ_RESPONSE
            );
            case COMMITTED -> {
                if (statusResponse.commitTimestamp() == null) {
                    sendResponse(
                            clientMessage,
                            new TxnReadResponse(null, false, hybridClock.now(),
                                    "Committed transaction is missing commit timestamp"),
                            TransactionalMessageTypes.TXN_READ_RESPONSE
                    );
                    return;
                }
                resolveCommittedIntent(
                        readRequest.key(), intentFromOtherTransaction, statusResponse.commitTimestamp());
                sendResponse(
                        clientMessage,
                        readCommittedAndRecordProvisional(readRequest, propagatedTime),
                        TransactionalMessageTypes.TXN_READ_RESPONSE
                );
            }
            case ABORTED -> {
                intentStore.delete(intentFromOtherTransaction.key());
                sendResponse(
                        clientMessage,
                        readCommittedAndRecordProvisional(readRequest, propagatedTime),
                        TransactionalMessageTypes.TXN_READ_RESPONSE
                );
            }
        }
    }

    private boolean losesTieBreak(TxnId currentTxnId, TxnId otherTxnId) {
        return currentTxnId.toString().compareTo(otherTxnId.toString()) > 0;
    }

    private Optional<String> findOwnIntentValue(String key, TxnId txnId) {
        Map<HybridTimestamp, byte[]> versionsUpTo =
                intentStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);
        HybridTimestamp latestTimestamp = null;
        String latestValue = null;

        for (Map.Entry<HybridTimestamp, byte[]> entry : versionsUpTo.entrySet()) {
            IntentRecord intentRecord = deserializePayload(entry.getValue(), IntentRecord.class);
            if (!intentRecord.txnId().equals(txnId)) {
                continue;
            }
            if (latestTimestamp == null || entry.getKey().compareTo(latestTimestamp) > 0) {
                latestTimestamp = entry.getKey();
                latestValue = intentRecord.value();
            }
        }

        return Optional.ofNullable(latestValue);
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
        Map<HybridTimestamp, byte[]> versionsUpTo =
                intentStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);

        HybridTimestamp latestTimestamp = null;
        IntentRecord latestIntent = null;
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

    private GetTransactionStatusResponse getTransactionStatus(
            GetTransactionStatusRequest request,
            HybridTimestamp propagatedTime
    ) {
        TxnRecord txnRecord = txnRecords.get(request.txnId());
        if (txnRecord == null) {
            return new GetTransactionStatusResponse(
                    null,
                    null,
                    hybridClock.now(),
                    "Transaction not found: " + request.txnId()
            );
        }
        return new GetTransactionStatusResponse(
                txnRecord.status(),
                txnRecord.commitTimestamp(),
                propagatedTime,
                null
        );
    }

    private Optional<StoredIntent> findStoredIntent(String key, TxnId txnId) {
        Map<HybridTimestamp, byte[]> versionsUpTo =
                intentStore.getVersionsUpTo(encodeLogicalKey(key), MAX_TIMESTAMP);

        HybridTimestamp latestTimestamp = null;
        IntentRecord latestIntent = null;
        for (Map.Entry<HybridTimestamp, byte[]> entry : versionsUpTo.entrySet()) {
            IntentRecord intentRecord = deserializePayload(entry.getValue(), IntentRecord.class);
            if (!intentRecord.txnId().equals(txnId)) {
                continue;
            }
            if (latestTimestamp == null || entry.getKey().compareTo(latestTimestamp) > 0) {
                latestTimestamp = entry.getKey();
                latestIntent = intentRecord;
            }
        }

        if (latestTimestamp == null || latestIntent == null) {
            return Optional.empty();
        }

        return Optional.of(new StoredIntent(intentKey(key, latestTimestamp), latestIntent));
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

    private HybridTimestamp mergeClock(HybridTimestamp remoteTimestamp) {
        return hybridClock.tick(remoteTimestamp);
    }

    private Timeout startedTimeout(TxnId txnId) {
        Timeout timeout = new Timeout("txn-" + txnId, timeoutTicks);
        timeout.start();
        return timeout;
    }

    private MVCCKey intentKey(String key, HybridTimestamp intentTimestamp) {
        return new MVCCKey(encodeLogicalKey(key), intentTimestamp);
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

    private byte[] encodeIntentRecord(TxnWriteRequest request) {
        return serializePayload(new IntentRecord(request.txnId(), request.value()));
    }

    private byte[] encodeCommittedValue(StoredIntent storedIntent) {
        return OrderPreservingCodec.encodeString(storedIntent.intentRecord().value());
    }

    private void sendResponse(Message message, Object response, MessageType responseType) {
        send(createResponseMessage(message, response, responseType));
    }

    private record StoredIntent(MVCCKey key, IntentRecord intentRecord) {
    }
}
