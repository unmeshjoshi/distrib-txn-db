package com.distrib.txn.kv;

import clock.HybridClock;
import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
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

    private final MVCCStore committedStore;
    private final MVCCStore intentStore;
    private final Map<TxnId, TxnRecord> txnRecords;
    private final HybridClock hybridClock;

    public TransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            List<ProcessId> peerIds,
            ProcessParams processParams
    ) {
        super(peerIds, processParams);
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
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                TransactionalMessageTypes.BEGIN_TRANSACTION_REQUEST, this::handleBeginTransactionRequest,
                TransactionalMessageTypes.TXN_WRITE_REQUEST, this::handleTxnWriteRequest,
                TransactionalMessageTypes.TXN_READ_REQUEST, this::handleTxnReadRequest,
                TransactionalMessageTypes.COMMIT_TRANSACTION_REQUEST, this::handleCommitTransactionRequest,
                TransactionalMessageTypes.RESOLVE_TRANSACTION_REQUEST, this::handleResolveTransactionRequest
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
        TxnWriteResponse response = writeIntent(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.TXN_WRITE_RESPONSE);
    }

    private void handleTxnReadRequest(Message message) {
        TxnReadRequest request = deserializePayload(message.payload(), TxnReadRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        TxnReadResponse response = read(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.TXN_READ_RESPONSE);
    }

    private void handleCommitTransactionRequest(Message message) {
        CommitTransactionRequest request = deserializePayload(message.payload(), CommitTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        CommitTransactionResponse response = commit(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.COMMIT_TRANSACTION_RESPONSE);
    }

    private void handleResolveTransactionRequest(Message message) {
        ResolveTransactionRequest request = deserializePayload(message.payload(), ResolveTransactionRequest.class);
        HybridTimestamp propagatedTime = mergeClock(request.clientTime());
        ResolveTransactionResponse response = resolve(request, propagatedTime);
        sendResponse(message, response, TransactionalMessageTypes.RESOLVE_TRANSACTION_RESPONSE);
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
                    propagatedTime,
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

    private TxnReadResponse read(TxnReadRequest request, HybridTimestamp propagatedTime) {
        try {
            Optional<String> ownIntentValue = findOwnIntentValue(request);
            if (ownIntentValue.isPresent()) {
                return new TxnReadResponse(ownIntentValue.get(), true, propagatedTime, null);
            }

            Optional<String> committedValue = readCommittedValue(request);
            if (committedValue.isPresent()) {
                return new TxnReadResponse(committedValue.get(), true, propagatedTime, null);
            }

            return new TxnReadResponse(null, false, propagatedTime, null);
        } catch (Exception e) {
            return new TxnReadResponse(null, false, hybridClock.now(), e.getMessage());
        }
    }

    private CommitTransactionResponse commit(
            CommitTransactionRequest request,
            HybridTimestamp commitTimestamp
    ) {
        try {
            TxnRecord txnRecord = txnRecords.get(request.txnId());
            if (txnRecord == null) {
                return new CommitTransactionResponse(
                        false, hybridClock.now(), null, "Transaction not found: " + request.txnId()
                );
            }

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
                    commitTimestamp,
                    txnRecord.isolationLevel()
            ));

            sendResolveRequests(request.txnId(), request.participantWrites(), commitTimestamp);
            return new CommitTransactionResponse(true, commitTimestamp, commitTimestamp, null);
        } catch (Exception e) {
            return new CommitTransactionResponse(false, hybridClock.now(), null, e.getMessage());
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

    private MVCCKey versionedKey(String key, HybridTimestamp timestamp) {
        return new MVCCKey(encodeLogicalKey(key), timestamp);
    }

    private byte[] encodeLogicalKey(String key) {
        return OrderPreservingCodec.encodeString(key);
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
