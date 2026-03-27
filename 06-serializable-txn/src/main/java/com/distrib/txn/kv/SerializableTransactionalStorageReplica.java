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
        HybridTimestamp intentTimestamp = mergeClock(request.clientTime());

        Optional<TxnId> conflictingTxn = conflictingPendingReadTxn(request.key(), request.txnId());
        if (conflictingTxn.isPresent() && losesTieBreak(request.txnId(), conflictingTxn.get())) {
            sendResponse(
                    message,
                    new TxnWriteResponse(false, hybridClock.now(), "Conflicting serializable read"),
                    TransactionalMessageTypes.TXN_WRITE_RESPONSE
            );
            return;
        }

        TxnWriteResponse response = writeIntent(request, intentTimestamp);
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

    private TxnReadResponse read(TxnReadRequest request, HybridTimestamp propagatedTime) {
        try {
            Optional<String> ownIntentValue = findOwnIntentValue(request.key(), request.txnId());
            if (ownIntentValue.isPresent()) {
                return new TxnReadResponse(ownIntentValue.get(), true, propagatedTime, null);
            }

            Optional<String> committedValue = committedStore
                    .getAsOf(versionedKey(request.key(), request.readTimestamp()))
                    .map(OrderPreservingCodec::decodeString);

            recordReadProvisional(request, propagatedTime);

            if (committedValue.isPresent()) {
                return new TxnReadResponse(committedValue.get(), true, propagatedTime, null);
            }
            return new TxnReadResponse(null, false, propagatedTime, null);
        } catch (Exception e) {
            return new TxnReadResponse(null, false, hybridClock.now(), e.getMessage());
        }
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
