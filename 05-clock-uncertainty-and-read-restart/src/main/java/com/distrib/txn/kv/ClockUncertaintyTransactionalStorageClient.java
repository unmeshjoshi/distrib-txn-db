package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.future.ListenableFuture;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClockUncertaintyTransactionalStorageClient extends TransactionalStorageClient {
    private final Map<TxnId, HybridTimestamp> restartedReadTimestamps;

    public ClockUncertaintyTransactionalStorageClient(List<ProcessId> replicas, ProcessParams processParams) {
        super(replicas, processParams);
        this.restartedReadTimestamps = new HashMap<>();
    }

    @Override
    protected ListenableFuture<TxnReadResponse> read(
            TxnId txnId,
            String key,
            HybridTimestamp readTimestamp,
            HybridTimestamp clientTime
    ) {
        ListenableFuture<TxnReadResponse> result = new ListenableFuture<>();
        readWithRestart(result, txnId, key, effectiveReadTimestamp(txnId, readTimestamp), clientTime);
        return result;
    }

    private void readWithRestart(
            ListenableFuture<TxnReadResponse> result,
            TxnId txnId,
            String key,
            HybridTimestamp readTimestamp,
            HybridTimestamp clientTime
    ) {
        super.read(txnId, key, readTimestamp, clientTime).handle((response, error) -> {
            if (error != null) {
                result.fail(error);
                return;
            }

            if (requiresRestart(response)) {
                rememberRestart(txnId, response.propagatedTime());
                readWithRestart(result, txnId, key, response.propagatedTime(), response.propagatedTime());
                return;
            }

            result.complete(response);
        });
    }

    private boolean requiresRestart(TxnReadResponse response) {
        return response != null && ReadRestart.REQUIRED.equals(response.error());
    }

    private void rememberRestart(TxnId txnId, HybridTimestamp restartedReadTimestamp) {
        restartedReadTimestamps.merge(txnId, restartedReadTimestamp, this::laterTimestamp);
    }

    private HybridTimestamp effectiveReadTimestamp(TxnId txnId, HybridTimestamp requestedReadTimestamp) {
        HybridTimestamp restartedReadTimestamp = restartedReadTimestamps.get(txnId);
        if (restartedReadTimestamp == null) {
            return requestedReadTimestamp;
        }
        return laterTimestamp(restartedReadTimestamp, requestedReadTimestamp);
    }

    private HybridTimestamp laterTimestamp(HybridTimestamp left, HybridTimestamp right) {
        return left.compareTo(right) >= 0 ? left : right;
    }
}
