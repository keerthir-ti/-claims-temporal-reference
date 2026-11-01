package claims.lifecycle.workflows;

import static claims.contracts.Stage.STAGE_CLOSED;
import static claims.contracts.Stage.STAGE_ESCALATION;
import static claims.contracts.Stage.STAGE_MANUAL;
import static claims.contracts.Stage.STAGE_REOPEN_REVIEW;
import static claims.contracts.Stage.STAGE_TRIAGE;

import claims.contracts.CancelPayment;
import claims.contracts.ClaimEvent;
import claims.contracts.ClaimSnapshot;
import claims.contracts.ClaimStart;
import claims.contracts.ClaimState;
import claims.contracts.CloseClaimRequest;
import claims.contracts.Decision;
import claims.contracts.DecisionResult;
import claims.contracts.PolicyRequest;
import claims.contracts.RecordCheck;
import claims.contracts.Stage;
import claims.contracts.StageChange;
import claims.contracts.StepRef;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;
import claims.runtime.Names;
import claims.runtime.SearchAttrs;
import claims.runtime.activities.ProjectionActivities;
import claims.runtime.workflow.ActivityStubs;
import claims.runtime.workflow.DecisionDesk;
import claims.runtime.workflow.HistoryRecorder;
import claims.runtime.workflow.StepRunner;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * The claim, as one entity workflow. The stage machine runs the module of the current stage (triage,
 * investigation, STP) or the human decision of a human stage, then applies {@link Routing}. A claim closed or
 * reassigned in ClaimsPro cancels the running module and withdraws its open tasks; a payment in progress is
 * never cancelled and is awaited before the workflow completes. Continue-As-New only between stages, with no
 * decision and no payment open. The workflow completes when the claim closes; a reopen is a new run.
 */
public class ClaimWorkflowImpl implements ClaimWorkflow {
  private static final Logger log = Workflow.getLogger(ClaimWorkflowImpl.class);
  private static final int MAX_SEEN_EVENTS = 500;
  private static final int HISTORY_EVENT_LIMIT = 5_000;

  // Touched by handlers, which can run before run() has initialised.
  private final LinkedHashSet<String> seen = new LinkedHashSet<>();
  private String closedTrigger;
  private String reassignedTrigger;
  private String reopenTrigger;
  private boolean changed;
  private boolean estimate;
  private boolean documents;
  private CancelPayment cancel;

  private final ProjectionActivities projection = ActivityStubs.projection();
  private ClaimState.Builder state;
  private HistoryRecorder recorder;
  private DecisionDesk desk;
  private ClaimContext c;
  private ProjectionActivities verifier;

  @Override
  public void run(ClaimStart start) {
    String claimId = start.getClaimId();
    recorder = new HistoryRecorder(claimId, Names.CLAIM);
    state = ClaimState.newBuilder().setClaimId(claimId);
    desk = new DecisionDesk(claimId, () -> state.getStageVersion(), recorder);
    c = new ClaimContext(claimId, () -> state.getStageVersion(), recorder, desk);
    deliverFlags();

    if (start.hasCarried()) {
      state = start.getCarried().toBuilder().clearSeenEventIds();
      seen.addAll(start.getCarried().getSeenEventIdsList());
      usePolicy(start.getCarried().getPolicy());
    } else if (!begin(start)) {
      return;
    }

    while (true) {
      if (state.getStage() != STAGE_CLOSED && closedTrigger != null) {
        enter(STAGE_CLOSED, "closed in ClaimsPro", closedTrigger);
      } else if (state.getStage() != STAGE_CLOSED && reassignedTrigger != null) {
        String trigger = reassignedTrigger;
        reassignedTrigger = null;
        enter(STAGE_MANUAL, "reassigned in ClaimsPro", trigger);
      }
      if (state.getStage() == STAGE_CLOSED) {
        if (reopenedWhileClosing()) {
          continue;
        }
        return;
      }
      if (!desk.hasOpen() && paymentsDone() && (Workflow.getInfo().isContinueAsNewSuggested()
          || Workflow.getInfo().getHistoryLength() > HISTORY_EVENT_LIMIT)) {
        continueAsNew();
        return;
      }
      try {
        apply(runStage());
      } catch (StepRunner.Stopped stopped) {
        enter(STAGE_MANUAL, stopped.getMessage(), "");
      }
    }
  }

  // ----------------------------------------------------------------------------------------- start

  private boolean begin(ClaimStart start) {
    if (!start.getStartEventId().isEmpty()) {
      seen.add(start.getStartEventId());
    }
    usePolicy(ActivityStubs.policy().load(PolicyRequest.newBuilder().setClaimId(start.getClaimId()).build()));
    state.setReopened(start.getReopened());
    state.setStageVersion(projection.currentStage(c.ref()).getStageVersion()); // a reopen continues the numbering
    ClaimSnapshot claim = c.steps.run("claim.read", () -> c.claimsPro.readClaim(c.ref()));
    if ("CLOSED".equals(claim.getStatus()) && !start.getReopened()) {
      recorder.row(state.getStageVersion(), "claim.ignored:" + start.getStartEventId())
          .outcome("ALREADY_CLOSED").trigger(start.getStartEventId()).write();
      recorder.closing(state.getStageVersion(), "IGNORED");
      return false;
    }
    enter(start.getReopened() ? STAGE_REOPEN_REVIEW : STAGE_TRIAGE,
        start.getReopened() ? "claim reopened" : "claim created", start.getStartEventId());
    return true;
  }

  private void usePolicy(claims.contracts.DecisionPolicy policy) {
    state.setPolicy(policy);
    c.usePolicy(policy);
    verifier = ActivityStubs.on(ProjectionActivities.class, Names.LIFECYCLE_QUEUE, Duration.ofSeconds(10),
        ActivityStubs.retryWindow(policy));
  }

  // ---------------------------------------------------------------------------------------- stages

  private Routing.Next runStage() {
    Stage stage = state.getStage();
    return switch (stage) {
      case STAGE_TRIAGE -> afterModule(stage, runModule("triage", () -> new TriageModule(c).run()));
      case STAGE_INVESTIGATION -> afterModule(stage, runModule("investigation", () -> new InvestigationModule(c).run()));
      case STAGE_STP -> afterModule(stage, runModule("stp", () -> new StpModule(c).run(state.getRouteReason())));
      case STAGE_MANUAL, STAGE_ESCALATION, STAGE_SIU, STAGE_REOPEN_REVIEW -> {
        DecisionResult d = desk.decide(Routing.decisionKind(stage), Routing.choices(stage),
            state.getRouteReason(), 0, "", this::interrupted);
        yield d == null ? null : Routing.afterDecision(stage, d.getChoice());
      }
      default -> Routing.Next.to(STAGE_MANUAL, "unknown stage " + stage);
    };
  }

  private static Routing.Next afterModule(Stage stage, TrackResult r) {
    return r == null ? null : Routing.afterTrack(stage, r);
  }

  /**
   * Runs a module in its own cancellation scope. Returns null when the claim was closed or reassigned in
   * ClaimsPro meanwhile: the module is cancelled and its open tasks withdrawn.
   */
  private TrackResult runModule(String name, Supplier<TrackResult> module) {
    int v = state.getStageVersion();
    List<Promise<TrackResult>> result = new ArrayList<>(1);
    CancellationScope scope = Workflow.newCancellationScope(() -> result.add(Async.function(module::get)));
    scope.run();
    Workflow.await(() -> result.get(0).isCompleted() || interrupted());
    if (!result.get(0).isCompleted()) {
      scope.cancel(closedTrigger != null ? "claim closed in ClaimsPro" : "claim reassigned in ClaimsPro");
      try {
        result.get(0).get();
      } catch (TemporalFailure | StepRunner.Stopped cancelled) {
        // cancelled as requested
      }
      Workflow.newDetachedCancellationScope(() -> {
        desk.withdrawAll();
        recorder.row(v, "module.cancelled:" + name).outcome("CANCELLED").write();
      }).run();
      return null;
    }
    try {
      return result.get(0).get();
    } catch (TemporalFailure failed) {
      log.warn("Module {} failed: {}", name, failed.getMessage());
      recorder.row(v, "module.failed:" + name).outcome("FAILED").reason(failed.getMessage()).write();
      return TrackResult.newBuilder().setOutcome(TrackOutcome.ROUTE_MANUAL).setReason(name + " failed").build();
    }
  }

  private void apply(Routing.Next next) {
    if (next == null) {
      return;
    }
    if (next.closeInClaimsPro()) {
      int v = state.getStageVersion();
      c.steps.run("claim.close", () -> c.claimsPro.closeClaim(CloseClaimRequest.newBuilder()
          .setStep(step(v, "claim.close")).setReason(next.reason()).build()));
    }
    if (next.retriage() && state.getPolicy().getPilotStpOnly()) {
      enter(STAGE_MANUAL, next.reason() + " (pilot scope: no triage; an adjuster decides)", "");
      return;
    }
    if (next.retriage()) {
      state.setRetriageCount(state.getRetriageCount() + 1);
      int limit = state.getPolicy().getRetriageLimit() > 0 ? state.getPolicy().getRetriageLimit() : 3;
      if (state.getRetriageCount() > limit) {
        enter(STAGE_ESCALATION, "re-triage limit " + limit + " reached: " + next.reason(), "");
        return;
      }
    }
    enter(next.stage(), next.reason(), "");
  }

  private boolean interrupted() {
    return closedTrigger != null || reassignedTrigger != null;
  }

  // ----------------------------------------------------------------------------------- bookkeeping

  private void enter(Stage stage, String reason, String trigger) {
    if (stage == STAGE_TRIAGE && state.getPolicy().getPilotStpOnly()) {
      stage = Stage.STAGE_STP; // pilot scope: Claims Lifecycle -> STP only
      reason = reason + " (pilot scope: straight to STP)";
    }
    int v = state.getStageVersion() + 1;
    projection.beginStage(StageChange.newBuilder().setStep(step(v, "stage.begin")).setStage(stage).build());
    state.setStage(stage).setStageVersion(v).setRouteReason(reason == null ? "" : reason);
    Workflow.upsertTypedSearchAttributes(SearchAttrs.STAGE.valueSet(stage.name().replace("STAGE_", "")));
    recorder.row(v, "stage:" + stage.name()).outcome(stage.name()).reason(reason).trigger(trigger).write();
  }

  private boolean paymentsDone() {
    return c.payments.stream().allMatch(Promise::isCompleted);
  }

  /**
   * The claim is closed: wait for any payment in progress, check the business record, then complete. A reopen that
   * arrives meanwhile (Signal-With-Start reaches this run while it is still open) is reviewed in this run.
   */
  private boolean reopenedWhileClosing() {
    int v = state.getStageVersion();
    Workflow.await(this::paymentsDone); // a payment is never stopped halfway
    try {
      c.steps.run("record.verify", () -> verifier.verifyRecord(RecordCheck.newBuilder()
          .setClaimId(state.getClaimId()).setCurrentRunId(Workflow.getInfo().getRunId())
          .setCurrentSeq(recorder.lastSeq()).build()));
    } catch (StepRunner.Stopped accepted) {
      recorder.row(v, "record.incomplete").outcome("ACCEPTED_BY_OPERATIONS").write();
    }
    Workflow.await(Workflow::isEveryHandlerFinished);
    if (reopenTrigger != null) {
      String trigger = reopenTrigger;
      reopenTrigger = null;
      closedTrigger = null;
      reassignedTrigger = null;
      enter(STAGE_REOPEN_REVIEW, "claim reopened while the workflow was closing", trigger);
      return true;
    }
    recorder.closing(v, "CLOSED");
    return false;
  }

  private void continueAsNew() {
    Workflow.await(Workflow::isEveryHandlerFinished);
    recorder.closing(state.getStageVersion(), "CONTINUED_AS_NEW");
    List<String> lastSeen = new ArrayList<>(seen);
    ClaimState carried = state.clone()
        .addAllSeenEventIds(lastSeen.subList(Math.max(0, lastSeen.size() - MAX_SEEN_EVENTS), lastSeen.size())).build();
    Workflow.continueAsNew(ClaimStart.newBuilder().setClaimId(state.getClaimId()).setCarried(carried).build());
  }

  private StepRef step(int v, String name) {
    return StepRef.newBuilder().setClaimId(state.getClaimId()).setStageVersion(v).setStep("claim." + name).build();
  }

  /** Events that arrived before run() initialised the context. */
  private void deliverFlags() {
    c.claimChanged = changed;
    c.estimateReceived = estimate;
    c.documentsReceived = documents;
    if (cancel != null) {
      c.cancelPaymentRequested = true;
      c.cancelPaymentBy = cancel.getRequestedBy();
    }
  }

  // -------------------------------------------------------------------------------------- messages

  @Override
  public void claimEvent(ClaimEvent e) {
    if (!seen.add(e.getEventId())) {
      return;
    }
    if (seen.size() > MAX_SEEN_EVENTS * 2) {
      seen.remove(seen.iterator().next());
    }
    switch (e.getType()) {
      case CLAIM_CREATED -> { } // the start event of this run
      case CLAIM_REOPENED -> {
        if (state != null && state.getStage() == STAGE_CLOSED) {
          reopenTrigger = e.getEventId(); // reopened while this run is closing
        }
      }
      case PAYMENT_CANCEL_REQUESTED -> cancelPayment(CancelPayment.newBuilder().setRequestedBy("ClaimsPro")
          .setReason("payment cancelled in ClaimsPro (event " + e.getEventId() + ")").build());
      case CLAIM_CLOSED -> {
        closedTrigger = e.getEventId();
        cancelPayment(CancelPayment.newBuilder().setRequestedBy("ClaimsPro")
            .setReason("claim closed in ClaimsPro (event " + e.getEventId() + ")").build());
      }
      case CLAIM_REASSIGNED -> reassignedTrigger = e.getEventId();
      case CLAIM_UPDATED -> {
        changed = true;
        if (c != null) {
          c.claimChanged = true;
        }
      }
      case ESTIMATE_RECEIVED -> {
        estimate = true;
        if (c != null) {
          c.estimateReceived = true;
        }
      }
      case DOCUMENTS_RECEIVED -> {
        documents = true;
        if (c != null) {
          c.documentsReceived = true;
        }
      }
      default -> { }
    }
  }

  @Override
  public void cancelPayment(CancelPayment cancelPayment) {
    cancel = cancelPayment;
    if (c != null) {
      c.cancelPaymentRequested = true;
      c.cancelPaymentBy = cancelPayment.getRequestedBy();
    }
  }

  @Override
  public void validateDecide(Decision decision) {
    if (desk == null) {
      throw new IllegalStateException("Workflow is starting; retry");
    }
    desk.validate(decision);
  }

  @Override
  public DecisionResult decide(Decision decision) {
    return desk.handle(decision);
  }
}
