package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.List;
import java.util.Map;

public class ClockUncertaintyTransactionalStorageReplica extends TransactionalStorageReplica {
    public static final long DEFAULT_UNCERTAINTY_OFFSET = 10;

    private final long uncertaintyOffset;

    public ClockUncertaintyTransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            List<ProcessId> peerIds,
            ProcessParams processParams
    ) {
        this(committedStore, intentStore, peerIds, processParams, DEFAULT_UNCERTAINTY_OFFSET);
    }

    public ClockUncertaintyTransactionalStorageReplica(
            MVCCStore committedStore,
            MVCCStore intentStore,
            List<ProcessId> peerIds,
            ProcessParams processParams,
            long uncertaintyOffset
    ) {
        super(committedStore, intentStore, peerIds, processParams);
        this.uncertaintyOffset = uncertaintyOffset;
    }

    @Override
    protected TxnReadResponse readCommitted(TxnReadRequest request, HybridTimestamp propagatedTime) {
        HybridTimestamp uncertaintyLimit = new HybridTimestamp(
                request.readTimestamp().getWallClockTime() + uncertaintyOffset,
                request.readTimestamp().getTicks()
        );

        HybridTimestamp latestCommittedTimestamp =
                latestCommittedTimestamp(request.key(), uncertaintyLimit);

        if (latestCommittedTimestamp != null
                && latestCommittedTimestamp.compareTo(request.readTimestamp()) > 0) {
            return new TxnReadResponse(null, false, latestCommittedTimestamp, ReadRestart.REQUIRED);
        }

        return super.readCommitted(request, propagatedTime);
    }

    private HybridTimestamp latestCommittedTimestamp(String key, HybridTimestamp asOfTime) {
        Map<HybridTimestamp, byte[]> versions =
                committedStore().getVersionsUpTo(OrderPreservingCodec.encodeString(key), asOfTime);

        HybridTimestamp latestTimestamp = null;
        for (HybridTimestamp timestamp : versions.keySet()) {
            if (latestTimestamp == null || timestamp.compareTo(latestTimestamp) > 0) {
                latestTimestamp = timestamp;
            }
        }
        return latestTimestamp;
    }
}
