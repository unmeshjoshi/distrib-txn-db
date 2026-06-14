package com.distrib.txn.kv.dsl;

import com.distrib.txn.kv.BeginTransactionResponse;
import com.distrib.txn.kv.CommitTransactionResponse;
import com.distrib.txn.kv.IsolationLevel;
import com.distrib.txn.kv.TxnId;
import com.distrib.txn.kv.TxnReadResponse;
import com.distrib.txn.kv.TxnWriteResponse;
import com.tickloom.testkit.dsl.ActionScope;
import com.tickloom.testkit.dsl.EventOrAwaitScope;

public interface TxnActionScope extends ActionScope {
    EventOrAwaitScope<TxnActionScope, BeginTransactionResponse> beginTransaction(TxnId txnId, IsolationLevel isolationLevel);
    EventOrAwaitScope<TxnActionScope, TxnWriteResponse> writes(TxnId txnId, String key, String value);
    EventOrAwaitScope<TxnActionScope, TxnReadResponse> reads(TxnId txnId, String key);
    EventOrAwaitScope<TxnActionScope, CommitTransactionResponse> commits(TxnId txnId);
}
