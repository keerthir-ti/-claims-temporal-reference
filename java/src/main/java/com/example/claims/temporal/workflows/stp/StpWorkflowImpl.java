package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.Names;
import com.example.claims.temporal.SearchAttrs;
import com.example.claims.temporal.activities.ActivityErrors;
import com.example.claims.temporal.activities.ClaimSnapshot;
import com.example.claims.temporal.activities.ProjectionActivities;
import com.example.claims.temporal.activities.StepContext;
import com.example.claims.temporal.activities.StepResult;
import com.example.claims.temporal.activities.StpActivities;
import com.example.claims.temporal.workflows.stp.flow.FlowDefinition;
import com.example.claims.temporal.workflows.stp.flow.FlowInvariants;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.ProjectionRecord;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackOutcome;
import com.example.claims.contracts.TrackRequest;
import com.example.claims.contracts.TrackResult;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;

/**
 * STP track: interprets the claim's flow definition stage by stage.
 *
 * <p>Continuous monitoring: an event is only a hint. On CLAIM_UPDATED the workflow re-reads
 * ClaimsPro and changes course only if {@link MaterialChange} says so. Missed events are covered by
 * re-reading at every stage boundary and by polling while waiting for an estimate.
 */
public class StpWorkflowImpl implements StpWorkflow {
  private static final Logger log = Workflow.getLogger(StpWorkflowImpl.class);

  private final StpActivities activities = Workflow.newActivityStub(StpActivities.class,
      ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofSeconds(30))
          .setRetryOptions(RetryOptions.newBuilder()
              .setInitialInterval(Duration.ofSeconds(1))
              .setMaximumInterval(Duration.ofSeconds(30))
              .setDoNotRetry(ActivityErrors.STALE_STAGE, ActivityErrors.NOT_FOUND, ActivityErrors.BUSINESS_RULE)
              .build())
          .build());
  private final ProjectionActivities projection = Workflow.newActivityStub(ProjectionActivities.class,
      ActivityOptions.newBuilder()
          .setTaskQueue(Names.PROJECTION_QUEUE)
          .setStartToCloseTimeout(Duration.ofSeconds(30))
          .build());

  private TrackRequest request;
  private LifecycleSettings settings;
  private ClaimSnapshot snapshot;
  private long snapshotReadAt;
  private FlowDefinition flow;

  private final Deque<ClaimEvent> inbox = new ArrayDeque<>();
  private final Set<String> seenEventIds = new HashSet<>();
  private final List<String> completedSteps = new ArrayList<>();
  /** Projection writes run in the background and are awaited before the track completes. */
  private final List<Promise<Void>> projectionWrites = new ArrayList<>();

  private String currentStage;
  private String waitingFor;
  private String retriageReason;
  private boolean estimateSignalled;
  private boolean cancelPaymentRequested;
  private long estimateCents;
  private String paymentId;

  @Override
  public TrackResult run(TrackRequest req) {
    request = req;
    settings = req.getSettings() != null ? req.getSettings() : new LifecycleSettings();
    snapshot = activities.fetchClaim(req.getClaimId());
    snapshotReadAt = Workflow.currentTimeMillis();
    flow = activities.resolveFlow(snapshot);
    Workflow.upsertTypedSearchAttributes(SearchAttrs.STP_FLOW.valueSet(flow.ref()));
    record("FLOW_SELECTED", "OK", flow.ref() + " hash=" + flow.contentHash());

    // Events that arrived while no STP child was running are processed like live ones.
    inbox.addAll(req.getBufferedEvents());

    for (FlowDefinition.Stage stage : flow.stages()) {
      currentStage = stage.name();
      revalidateIfStale();
      processInbox();
      if (retriageReason != null) {
        return finish(TrackOutcome.RETRIAGE_REQUESTED, retriageReason);
      }
      TrackResult terminal = runStage(stage);
      if (terminal != null) {
        return terminal;
      }
    }
    return finish(TrackOutcome.COMPLETED, "flow " + flow.ref() + " completed");
  }

  /** @return a terminal result, or null to continue with the next stage */
  private TrackResult runStage(FlowDefinition.Stage stage) {
    boolean interruptible = FlowInvariants.interruptible(stage);
    List<Promise<StepResult>> steps = new ArrayList<>();
    CancellationScope scope = Workflow.newCancellationScope(() -> {
      for (String step : stage.steps()) {
        steps.add(Async.function(this::runStep, step));
      }
    });
    scope.run();
    Promise<Void> all = Promise.allOf(steps);

    while (true) {
      Workflow.await(() -> all.isCompleted() || !inbox.isEmpty());
      processInbox();
      if (retriageReason != null && interruptible && !all.isCompleted()) {
        // Important change mid-stage: stop in-flight work. Stages that touch money are never
        // interruptible; for them we let the stage finish and re-triage at the boundary.
        scope.cancel(retriageReason);
        return finish(TrackOutcome.RETRIAGE_REQUESTED, retriageReason);
      }
      if (all.isCompleted()) {
        break;
      }
    }

    for (int i = 0; i < steps.size(); i++) {
      StepResult r = steps.get(i).get();
      String step = stage.steps().get(i);
      switch (r.status()) {
        case "OK":
          completedSteps.add(step);
          break;
        case "RETRIAGE":
          return finish(TrackOutcome.RETRIAGE_REQUESTED, step + ": " + r.detail());
        default:
          return finish(TrackOutcome.ADJUSTER_HANDOFF, step + ": " + r.detail());
      }
    }
    return null;
  }

  /** Runs one registered step. Never throws: failures become a HANDOFF result. */
  private StepResult runStep(String step) {
    StepContext ctx = new StepContext(request.getClaimId(), request.getStageVersion(), step);
    StepResult result;
    try {
      result = switch (step) {
        case "INITIAL_CONTACT" -> activities.initialContact(ctx);
        case "VERIFY_COVERAGE" -> activities.verifyCoverage(ctx);
        case "OPEN_RESERVE" -> activities.openReserve(ctx);
        case "ASSIGN_APPRAISAL" -> activities.assignAppraisal(ctx);
        case "VENDOR_DISPATCH" -> dispatchVendor(ctx);
        case "AWAIT_ESTIMATE" -> awaitEstimate(ctx);
        case "ISSUE_PAYMENT" -> issuePayment(ctx);
        case "FINALIZE_PAYMENT" -> finalizePayment(ctx);
        case "CLOSE_CLAIM" -> activities.closeClaim(ctx);
        default -> StepResult.handoff("unregistered step " + step);
      };
    } catch (CanceledFailure e) {
      result = new StepResult("CANCELLED", "cancelled: " + e.getMessage(), null, null);
    } catch (ActivityFailure | ChildWorkflowFailure e) {
      String type = e.getCause() instanceof ApplicationFailure af ? af.getType() : "";
      if (ActivityErrors.STALE_STAGE.equals(type)) {
        log.warn("Step {} rejected by ClaimsPro: stage {} is stale", step, request.getStageVersion());
      }
      result = StepResult.handoff("failed: " + rootMessage(e));
    }
    record(step, result.status(), result.detail());
    return result;
  }

  private StepResult dispatchVendor(StepContext ctx) {
    VendorDispatchWorkflow dispatch = Workflow.newChildWorkflowStub(VendorDispatchWorkflow.class,
        ChildWorkflowOptions.newBuilder()
            .setWorkflowId(Workflow.getInfo().getWorkflowId() + ":dispatch")
            .build());
    return dispatch.run(ctx);
  }

  private StepResult awaitEstimate(StepContext ctx) {
    activities.requestEstimate(ctx);
    waitingFor = "ESTIMATE_RECEIVED";
    // Versioned change: the backstop poll was added after the first release. Histories recorded
    // before it replay down the DEFAULT_VERSION path unchanged.
    int version = Workflow.getVersion("estimate-backstop-poll", Workflow.DEFAULT_VERSION, 1);
    long deadline = Workflow.currentTimeMillis() + settings.getEventWaitSeconds() * 1000L;
    Duration poll = Duration.ofSeconds(settings.getRevalidateIntervalSeconds());
    boolean ready = false;
    while (!ready) {
      long remaining = deadline - Workflow.currentTimeMillis();
      if (remaining <= 0) {
        waitingFor = null;
        return StepResult.handoff("estimate not received within " + settings.getEventWaitSeconds() + "s");
      }
      Duration wait = version == 1 ? Duration.ofMillis(Math.min(poll.toMillis(), remaining)) : Duration.ofMillis(remaining);
      boolean signalled = Workflow.await(wait, () -> estimateSignalled);
      if (signalled || version == 1) {
        // Event or poll tick: ClaimsPro is the source of truth either way.
        StepResult status = activities.checkEstimate(ctx);
        ready = status.isOk();
        if (ready && !signalled) {
          log.warn("Backstop poll found the estimate; ESTIMATE_RECEIVED event was missed");
        }
        if (ready) {
          estimateCents = status.amountCents();
        }
      }
    }
    waitingFor = null;
    long authority = FlowInvariants.effectiveAuthority(flow);
    if (estimateCents > authority) {
      return StepResult.handoff("estimate " + estimateCents + " exceeds STP authority " + authority);
    }
    return StepResult.ok("estimate " + estimateCents, null, estimateCents);
  }

  private StepResult issuePayment(StepContext ctx) {
    if (cancelPaymentRequested) {
      return StepResult.handoff("payment cancel requested before issue");
    }
    StepResult r = activities.issuePayment(ctx, estimateCents);
    paymentId = r.ref();
    return r;
  }

  private StepResult finalizePayment(StepContext ctx) {
    if (cancelPaymentRequested) {
      // Branch: the cancel arrived between "create payment" and "close payment".
      activities.cancelPayment(ctx, paymentId);
      return StepResult.handoff("payment " + paymentId + " cancelled on request");
    }
    return activities.finalizePayment(ctx, paymentId);
  }

  // ---------------------------------------------------------------- monitoring

  @Override
  public void claimEvent(ClaimEvent event) {
    if (event.getEventId() != null && seenEventIds.add(event.getEventId())) {
      inbox.add(event);
    }
  }

  private void processInbox() {
    while (!inbox.isEmpty()) {
      ClaimEvent e = inbox.poll();
      switch (e.getEventType()) {
        case ESTIMATE_RECEIVED -> estimateSignalled = true;
        case PAYMENT_CANCEL_REQUESTED -> cancelPaymentRequested = true;
        case CLAIM_UPDATED -> revalidate("event " + e.getEventId());
        default -> log.info("STP ignores {}", e.getEventType());
      }
    }
  }

  private void revalidateIfStale() {
    long ageMs = Workflow.currentTimeMillis() - snapshotReadAt;
    if (ageMs >= settings.getRevalidateIntervalSeconds() * 1000L) {
      revalidate("stage boundary backstop");
    }
  }

  private void revalidate(String trigger) {
    ClaimSnapshot fresh = activities.fetchClaim(request.getClaimId());
    snapshotReadAt = Workflow.currentTimeMillis();
    if (fresh.version() <= snapshot.version()) {
      return;
    }
    String reason = MaterialChange.detect(snapshot, fresh);
    log.info("Revalidated ({}): version {} -> {}, material={}", trigger, snapshot.version(), fresh.version(), reason);
    snapshot = fresh;
    if (reason != null && retriageReason == null) {
      retriageReason = reason;
    }
  }

  // ---------------------------------------------------------------- results

  private TrackResult finish(TrackOutcome outcome, String reason) {
    record("STP_" + outcome.value(), "OK", reason);
    Promise.allOf(projectionWrites).get();
    return new TrackResult()
        .withSchemaVersion(1)
        .withClaimId(request.getClaimId())
        .withTrack(Track.STP)
        .withStageVersion(request.getStageVersion())
        .withOutcome(outcome)
        .withReason(reason)
        .withCompletedSteps(new ArrayList<>(completedSteps));
  }

  private void record(String step, String status, String detail) {
    ProjectionRecord row = new ProjectionRecord()
        .withSchemaVersion(1)
        .withClaimId(request.getClaimId())
        .withStageVersion(request.getStageVersion())
        .withTrack(Track.STP)
        .withStep(step)
        .withStatus(status)
        .withDetail(detail)
        .withWorkflowId(Workflow.getInfo().getWorkflowId())
        .withRunId(Workflow.getInfo().getRunId())
        .withOccurredAt(Instant.ofEpochMilli(Workflow.currentTimeMillis()).toString());
    // Detached: a step cancelled mid-stage must still get its projection row.
    Workflow.newDetachedCancellationScope(
        () -> projectionWrites.add(Async.procedure(projection::recordProjection, row))).run();
  }

  @Override
  public Progress getProgress() {
    return new Progress(flow == null ? null : flow.ref(), currentStage, List.copyOf(completedSteps), waitingFor, retriageReason);
  }

  private static String rootMessage(Throwable t) {
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.getMessage();
  }
}
