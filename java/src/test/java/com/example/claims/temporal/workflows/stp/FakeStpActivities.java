package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.workflows.stp.flow.FlowRegistryTest;
import com.example.claims.temporal.activities.ActivityErrors;
import com.example.claims.temporal.activities.ClaimSnapshot;
import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.activities.StepResult;
import com.example.claims.temporal.activities.StpActivities;
import com.example.claims.temporal.workflows.stp.flow.FlowDefinition;
import com.example.claims.temporal.workflows.stp.flow.FlowRegistry;
import io.temporal.failure.ApplicationFailure;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** In-memory ClaimsPro behaviour for workflow tests. Activities may be non-deterministic; workflows may not. */
class FakeStpActivities implements StpActivities {
  final AtomicReference<ClaimSnapshot> claim = new AtomicReference<>(
      FlowRegistryTest.claim("AUTO", "MA", "AGENT"));
  final List<String> calls = new CopyOnWriteArrayList<>();
  final AtomicInteger estimateChecks = new AtomicInteger();
  volatile long estimateCents = 100_000;
  volatile int estimateReadyAfterChecks = 1;
  volatile boolean staleStageOnReserve;
  /** Hook run inside issuePayment, e.g. to signal the workflow at exactly that moment. */
  volatile Consumer<StepContext> onIssuePayment = ctx -> {};

  private final FlowRegistry flows = FlowRegistry.load(Path.of(System.getProperty("flows.dir", "../config/flows")));

  @Override public ClaimSnapshot fetchClaim(String claimId) { calls.add("fetchClaim"); return claim.get(); }
  @Override public FlowDefinition resolveFlow(ClaimSnapshot c) { return flows.select(c); }
  @Override public StepResult initialContact(StepContext ctx) { calls.add(ctx.step()); return StepResult.ok("ok"); }
  @Override public StepResult verifyCoverage(StepContext ctx) { calls.add(ctx.step()); return StepResult.ok("ok"); }

  @Override
  public StepResult openReserve(StepContext ctx) {
    calls.add(ctx.step());
    if (staleStageOnReserve) {
      throw ApplicationFailure.newNonRetryableFailure("stage moved on", ActivityErrors.STALE_STAGE);
    }
    return StepResult.ok("ok", "R-1", 1000L);
  }

  @Override public StepResult assignAppraisal(StepContext ctx) { calls.add(ctx.step()); return StepResult.ok("ok"); }
  @Override public StepResult requestEstimate(StepContext ctx) { calls.add(ctx.step()); return StepResult.ok("ok"); }

  @Override
  public StepResult checkEstimate(StepContext ctx) {
    return estimateChecks.incrementAndGet() >= estimateReadyAfterChecks
        ? StepResult.ok("ready", null, estimateCents)
        : new StepResult("PENDING", "not yet", null, null);
  }

  @Override
  public StepResult issuePayment(StepContext ctx, long amountCents) {
    calls.add(ctx.step());
    onIssuePayment.accept(ctx);
    return StepResult.ok("paid", "P-1", amountCents);
  }

  @Override public StepResult finalizePayment(StepContext ctx, String id) { calls.add(ctx.step()); return StepResult.ok("ok", id, null); }
  @Override public StepResult cancelPayment(StepContext ctx, String id) { calls.add("CANCEL_PAYMENT"); return StepResult.ok("ok", id, null); }
  @Override public StepResult closeClaim(StepContext ctx) { calls.add(ctx.step()); return StepResult.ok("ok"); }
  @Override public List<String> listVendors(StepContext ctx) { return List.of("V-1", "V-2"); }

  @Override
  public StepResult offerDispatch(StepContext ctx, String vendorId) {
    calls.add("OFFER:" + vendorId);
    return "V-2".equals(vendorId) ? StepResult.ok("yes", vendorId, null) : new StepResult("DECLINED", "no", vendorId, null);
  }
}
