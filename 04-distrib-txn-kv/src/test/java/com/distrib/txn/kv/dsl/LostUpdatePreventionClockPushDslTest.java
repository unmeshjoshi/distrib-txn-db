package com.distrib.txn.kv.dsl;

import clock.HybridTimestamp;
import com.distrib.txn.kv.BeginTransactionResponse;
import com.distrib.txn.kv.IsolationLevel;
import com.distrib.txn.kv.TransactionalStorageClient;
import com.distrib.txn.kv.TransactionalStorageReplica;
import com.distrib.txn.kv.TxnId;
import com.distrib.txn.kv.TxnWriteResponse;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import com.tickloom.testkit.dsl.semanticmodel.Scenario;
import kv.MVCCKey;
import kv.OrderPreservingCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lost-update prevention under Snapshot Isolation, driven by Hybrid Logical Clock
 * propagation — a minimal, end-to-end model of the two YugabyteDB primitives that
 * make SI safe across clock skew.
 *
 * <h2>The two YugabyteDB rules this exercises</h2>
 * <ol>
 *   <li><b>HLC travels on every RPC.</b> "On any RPC communication between two nodes,
 *       HLC values are exchanged. The node with the lower HLC updates its HLC to the
 *       higher value." Our replicas implement exactly this: every request handler calls
 *       {@code mergeClock(request.clientTime())} before doing any work, so a high
 *       timestamp seen on one node is dragged onto every node it then talks to.</li>
 *   <li><b>SI commit rule (first-committer-wins).</b> A transaction "can successfully
 *       commit only if no updates it has made conflict with any concurrent updates made
 *       by transactions that committed after that snapshot." Our
 *       {@code failsSnapshotIsolationWriteValidation} enforces this: a write is rejected
 *       if the committed store already holds a version of the key at a timestamp greater
 *       than the writer's snapshot.</li>
 * </ol>
 *
 * <h2>The race, and why the clock push is load-bearing</h2>
 * Bob runs on a <em>lagging</em> clock (1000); Alice on a <em>leading</em> clock (2000).
 * Both want to write the same key under SI.
 * <pre>
 *   bob.begin   ── snapshot ≈ 1000 (his coordinator lags)
 *   alice.begin ── snapshot ≈ 2000 (her coordinator leads)
 *   bob.write k ── provisional intent on the key owner (still at 1000)
 *   alice.read k ─ sees bob's PENDING intent. Under SI she ignores it and reads
 *                  committed data (→ not found). But the read first merged her 2000
 *                  HLC into the key owner, which then issues a status-check RPC to
 *                  bob's coordinator — carrying 2000 onto it (rule 1).
 *   bob.commit  ── commit timestamp is drawn from bob's coordinator clock, now &gt; 2000.
 *   alice.write k → REJECTED: a committed version now sits above her 2000 snapshot (rule 2).
 * </pre>
 *
 * <b>Counterfactual.</b> Without the push, bob's commit machinery — his coordinator and
 * his client are both seeded at 1000 — would commit at ~1000, <em>below</em> Alice's
 * 2000 snapshot. Alice's blind write would then find nothing newer than her snapshot,
 * succeed, and silently overwrite bob: the classic lost update. The HLC push is the only
 * thing standing between this scenario and that bug, which is why the test pins both the
 * "before" ({@code bobSeed < aliceSnapshot}) and the "after"
 * ({@code bobCommit > aliceSnapshot}) of the push.
 *
 * <p>Both end assertions are independently push-dependent: delete the
 * {@code mergeClock(...)} on the status-check handler (the line where bob's coordinator
 * absorbs the reader's HLC) and the run goes red — bob commits at ~1000, the ordering
 * assertion fails, and Alice's write is no longer rejected. The merge is the load-bearing
 * line, verified by removing it.
 *
 * <h2>What this model simplifies vs. real YugabyteDB</h2>
 * Real YB also guards against clock skew on the <em>read</em> side with an uncertainty
 * window ({@code max_clock_skew}) that can trigger a "read restart", and resolves
 * write-write conflicts with transaction priorities (abort/wait). Here we isolate a
 * single causal chain — read-triggered status RPC pushes the lagging committer above the
 * leading reader's snapshot — so the HLC-propagation primitive is the only mechanism that
 * can make the assertions pass.
 *
 * @see <a href="https://docs.yugabyte.com/stable/architecture/transactions/transactions-overview/">YugabyteDB transactions overview (HLC exchange on RPC)</a>
 * @see <a href="https://docs.yugabyte.com/stable/architecture/transactions/isolation-levels/">YugabyteDB isolation levels (SI commit rule)</a>
 */
public class LostUpdatePreventionClockPushDslTest {

    private static final ProcessId NODE1 = ProcessId.of("node1");
    private static final ProcessId NODE2 = ProcessId.of("node2");
    private static final ProcessId NODE3 = ProcessId.of("node3");
    private static final List<ProcessId> SERVERS = List.of(NODE1, NODE2, NODE3);

    private static final ProcessId BOB = ProcessId.of("bob");
    private static final ProcessId ALICE = ProcessId.of("alice");

    @Test
    void hlcPropagationPushesLaggingCommitAboveLeadingSnapshot() throws Exception {
        // Why brute-force: hash-based routing makes role assignment a deterministic function
        // of key/txnId, so we cannot pick role separations directly — we search IDs until the
        // resulting layout puts bob's coord, alice's coord, and the key owner on three distinct
        // nodes. That separation isolates the status-check RPC as the only path by which
        // alice's HLC can reach bob's coordinator, so the assertions can only pass if the
        // clock-push mechanism really fires.
        Routing routing = Routing.find(SERVERS,
                r -> !r.coord1().equals(r.keyOwner())
                  && !r.coord1().equals(r.coord2())
                  && !r.coord2().equals(r.keyOwner()));

        TxnId bobTxn = routing.txnId1();
        TxnId aliceTxn = routing.txnId2();
        String sharedKey = routing.key();

        HybridTimestamp bobLaggingClock = new HybridTimestamp(1_000L, 0);
        HybridTimestamp aliceLeadingClock = new HybridTimestamp(2_000L, 0);

        Captured captured = new Captured();

        Scenario<TransactionalStorageClient> s = TxnScenarios.scenario("LostUpdate HLC clock push")
                .servers(NODE1, NODE2, NODE3)
                .clients(BOB, ALICE)
                // Seed per-node clock skew by routing role (not NODE1/2/3 — hash routing
                // assigns those at runtime). A client's snapshot is its coordinator's merged
                // HLC, so the lead/lag must live on the nodes, not just the clients: bob's
                // coordinator and the key owner lag at 1000, alice's coordinator leads at 2000.
                // Bob's coordinator therefore starts provably below alice's snapshot, so the
                // only thing that can lift his commit above it is the read-triggered clock push.
                .given(g -> g
                        .nodeHlc(routing.coord1(), bobLaggingClock)
                        .nodeHlc(routing.keyOwner(), bobLaggingClock)
                        .nodeHlc(routing.coord2(), aliceLeadingClock)
                        .clientHlc(BOB, bobLaggingClock)
                        .clientHlc(ALICE, aliceLeadingClock))
                .steps(steps -> {
                    steps.client(BOB).beginTransaction(bobTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(BeginTransactionResponse::success);

                    steps.client(ALICE).beginTransaction(aliceTxn, IsolationLevel.SNAPSHOT)
                         .expectResponse(r -> {
                             // The snapshot is the COORDINATOR's merged HLC returned by
                             // beginTransaction (max of alice's coordinator node clock and
                             // alice's client clock), not alice's raw client HLC. The client
                             // reuses this exact value as the readTimestamp for her later reads
                             // and writes.
                             captured.aliceSnapshot = r.propagatedTime();
                             return r.success();
                         });

                    steps.client(BOB).writes(bobTxn, sharedKey, "100")
                         .expectResponse(TxnWriteResponse::success);

                    // Alice's read reaches the key owner, which merges alice's leading HLC and
                    // then issues the status-check RPC to bob's coordinator carrying that HLC —
                    // transitively dragging bob's lagging coordinator clock above alice's snapshot.
                    // Under SI she ignores bob's pending intent and reads committed data: not found.
                    steps.client(ALICE).reads(aliceTxn, sharedKey)
                         .expectResponse(r -> !r.found());

                    // Bob's commit picks a timestamp from the now-pushed coordinator HLC,
                    // landing above alice's snapshot.
                    steps.client(BOB).commits(bobTxn)
                         .expectResponse(r -> {
                             captured.bobCommitTs = r.commitTimestamp();
                             return r.success();
                         });

                    // Why await: the coordinator → key-owner resolve is fire-and-forget.
                    // Without this wait, alice's next write could race the in-flight resolve
                    // and we wouldn't deterministically hit the SI rejection path we are here
                    // to demonstrate.
                    steps.await(cluster -> hasCommittedValue(cluster, sharedKey));

                    // Capture alice's write outcome and always let the scenario proceed; the
                    // real check is the post-run assertion below, which fails with a precise
                    // message (a violated in-step predicate would only surface as a generic
                    // tick-timeout). This is the headline "lost update prevented" assertion.
                    steps.client(ALICE).writes(aliceTxn, sharedKey, "200")
                         .expectResponse(r -> {
                             captured.aliceWriteSucceeded = r.success();
                             captured.aliceWriteError = r.error();
                             return true;
                         });
                });

        s.run();

        // ---- before: bob's commit machinery starts strictly below alice's snapshot ----
        // Without this, "the push moved bob above alice" is unfalsifiable — bob might simply
        // have started above her. Pinning the precondition is what makes the push observable.
        assertTrue(bobLaggingClock.compareTo(captured.aliceSnapshot) < 0,
                "Precondition: bob's seed " + bobLaggingClock
                        + " must start below alice's snapshot " + captured.aliceSnapshot);

        // ---- after: the read-triggered HLC push forced bob's commit above alice's snapshot ----
        assertTrue(captured.bobCommitTs.compareTo(captured.aliceSnapshot) > 0,
                "Clock push must force bob's commit " + captured.bobCommitTs
                        + " above alice's snapshot " + captured.aliceSnapshot);

        // ---- outcome: the lost update is prevented by SI first-committer-wins ----
        assertFalse(captured.aliceWriteSucceeded,
                "Alice's blind write must be rejected — otherwise bob's commit is lost");
        assertEquals("Conflicting committed transaction", captured.aliceWriteError,
                "Rejection must come from SI write-write validation (a version committed "
                        + "above alice's snapshot), not from some unrelated failure");
    }

    /** Holds scenario observations captured inside step lambdas for post-run assertions. */
    private static final class Captured {
        HybridTimestamp bobCommitTs;
        HybridTimestamp aliceSnapshot;
        boolean aliceWriteSucceeded;
        String aliceWriteError;
    }

    private static boolean hasCommittedValue(Cluster cluster, String key) {
        HybridTimestamp upperBound = new HybridTimestamp(Long.MAX_VALUE, 0);
        MVCCKey lookup = new MVCCKey(OrderPreservingCodec.encodeString(key), upperBound);
        for (ProcessId node : SERVERS) {
            TransactionalStorageReplica replica =
                    (TransactionalStorageReplica) cluster.getProcess(node);
            if (replica.committedStore().getAsOf(lookup).isPresent()) {
                return true;
            }
        }
        return false;
    }
}
