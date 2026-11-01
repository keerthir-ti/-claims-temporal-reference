package claims.lifecycle.workflows;

import static claims.contracts.Stage.STAGE_CLOSED;
import static claims.contracts.Stage.STAGE_ESCALATION;
import static claims.contracts.Stage.STAGE_INVESTIGATION;
import static claims.contracts.Stage.STAGE_MANUAL;
import static claims.contracts.Stage.STAGE_REOPEN_REVIEW;
import static claims.contracts.Stage.STAGE_SIU;
import static claims.contracts.Stage.STAGE_STP;
import static claims.contracts.Stage.STAGE_TRIAGE;

import claims.contracts.CancelPayment;
import claims.contracts.ClaimEvent;
import claims.contracts.ClaimRef;
import claims.contracts.ClaimSnapshot;
import claims.contracts.ClaimStart;
import claims.contracts.ClaimState;
import claims.contracts.CloseClaimRequest;
import claims.contracts.Decision;
import claims.contracts.DecisionKind;
import claims.contracts.DecisionPolicy;
import claims.contracts.DecisionResult;
import claims.contracts.PaymentResult;
import claims.contracts.PolicyRequest;
import claims.contracts.RecordCheck;
import claims.contracts.Stage;
import claims.contracts.StageChange;
import claims.contracts.StepRef;
import claims.contracts.Track;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackRequest;
import claims.contracts.TrackResult;
import claims.runtime.Names;
import claims.runtime.SearchAttrs;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.activities.ProjectionActivities;
import claims.runtime.workflow.DecisionDesk;
import claims.runtime.workflow.HistoryRecorder;
import claims.runtime.workflow.StepRunner;
import claims.runtime.workflow.ActivityStubs;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.SearchAttributes;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowCancellationType;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import org.slf4j.Logger;

/**
 * Owns the claim's stage. Starts a new child workflow per track visit, never two at once; runs the
 * human stages (manual, escalation, SIU, reopen review) itself; completes when the claim closes.
 * Continue-As-New happens only between stages, when no child and no decision is open.
 */
public class ClaimLifecycleWorkflowImpl implements ClaimLifecycleWorkflow {
  private static final Logger log = Workflow.getLogger(ClaimLifecycleWorkflowImpl.class);
  private static final int MAX_SEEN_EVENTS = 500;
  private static final int MAX_PENDING_EVENTS = 50;
  private static final int HISTORY_EVENT_LIMIT = 5_000; // well below the 10,240-event warning

  // Touched by signal and update handlers, which can run before run() has initialised: field initialisers.
  private final LinkedHashSet<String> seen = new LinkedHashSet<>();
  private final Deque<ClaimEvent> toForward = new ArrayDeque<>();
  private final List<ClaimEvent> pending = new ArrayList<>();
  private final List<PaymentResult> payments = new ArrayList<>();
  private String activeChildId;
  private String closedTrigger;
  private String reassignedTrigger;
  private String reopenTrigger;
  /** Payment cancellations to forward to the claim's payment workflow (from ClaimsPro, or the claim closing). */
  private final Deque<String> paymentCancels = new ArrayDeque<>();
  private int paymentCancelCount;

  private final ProjectionActivities projection = ActivityStubs.projection();
  private ClaimState.Builder state;
  private HistoryRecorder recorder;
  private DecisionDesk desk;
  private StepRunner steps;
  private ClaimsProActivities claimsPro;
  private ProjectionActivities verifier;

  @Override
  public void run(ClaimStart start) {
    String claimId = start.getClaimId();
    recorder = new HistoryRecorder(claimId, Names.CLAIM_LIFECYCLE);
    state = ClaimState.newBuilder().setClaimId(claimId);
    desk = new DecisionDesk(claimId, () -> state.getStageVersion(), recorder);
    steps = new StepRunner(desk);

    if (start.hasCarried()) {
      resume(start.getCarried());
    } else if (!begin(start)) {
      return;
    }
    Async.procedure(this::forwardPaymentCancels);

    while (true) {
      if (closedTrigger != null && state.getStage() != STAGE_CLOSED) {
        enter(STAGE_CLOSED, "closed in ClaimsPro", closedTrigger);
      } else if (reassignedTrigger != null && state.getStage() != STAGE_CLOSED) {
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
      if (Workflow.getInfo().isContinueAsNewSuggested() || Workflow.getInfo().getHistoryLength() > HISTORY_EVENT_LIMIT) {
        continueAsNew();
        return;
      }
      try {
        runStage();
      } catch (StepRunner.Stopped stopped) {
        enter(STAGE_MANUAL, stopped.getMessage(), "");
      }
    }
  }

  // ----------------------------------------------------------------------------------------- start

  private boolean begin(ClaimStart start) {
    String claimId = start.getClaimId();
    if (!start.getStartEventId().isEmpty()) {
      seen.add(start.getStartEventId());
    }
    DecisionPolicy policy = ActivityStubs.policy().load(PolicyRequest.newBuilder().setClaimId(claimId).build());
    usePolicy(policy);
    state.setReopened(start.getReopened());
    state.setStageVersion(projection.currentStage(ref()).getStageVersion()); // a reopen continues the numbering

    ClaimSnapshot claim = steps.run("claim.read", () -> claimsPro.readClaim(ref()));
    if ("CLOSED".equals(claim.getStatus()) && !start.getReopened()) {
      // A late or duplicate "created" event for a closed claim: nothing to do.
      recorder.row(state.getStageVersion(), "claim.ignored:" + start.getStartEventId())
          .outcome("ALREADY_CLOSED").trigger(start.getStartEventId()).write();
      recorder.closing(state.getStageVersion(), "IGNORED");
      return false;
    }
    if (start.getReopened()) {
      enter(STAGE_REOPEN_REVIEW, "claim reopened", start.getStartEventId());
    } else {
      enter(STAGE_TRIAGE, "claim created", start.getStartEventId());
    }
    return true;
  }

  private void resume(ClaimState carried) {
    state = carried.toBuilder();
    seen.addAll(carried.getSeenEventIdsList());
    pending.addAll(carried.getPendingEventsList());
    payments.addAll(carried.getPaymentsList());
    usePolicy(carried.getPolicy());
    state.clearSeenEventIds().clearPendingEvents().clearPayments();
  }

  private void usePolicy(DecisionPolicy policy) {
    state.setPolicy(policy);
    desk.setPolicy(policy);
    Duration window = ActivityStubs.retryWindow(policy);
    claimsPro = ActivityStubs.claimsPro(window);
    verifier = Workflow.newActivityStub(ProjectionActivities.class,
        ActivityStubs.options(Names.LIFECYCLE_QUEUE, Duration.ofSeconds(10), window));
  }

  // ---------------------------------------------------------------------------------------- stages

  private void runStage() {
    switch (state.getStage()) {
      case STAGE_TRIAGE -> afterTrack(runTrack(Track.TRACK_TRIAGE, Names.TRIAGE, Names.TSA_QUEUE, "tsa"));
      case STAGE_INVESTIGATION -> afterTrack(runTrack(Track.TRACK_INVESTIGATION, Names.INVESTIGATION, Names.IS_QUEUE, "is"));
      case STAGE_STP -> afterTrack(runTrack(Track.TRACK_STP, Names.STP, Names.STP_QUEUE, "stp"));
      case STAGE_MANUAL -> manual();
      case STAGE_ESCALATION -> escalation();
      case STAGE_SIU -> siu();
      case STAGE_REOPEN_REVIEW -> reopenReview();
      default -> enter(STAGE_MANUAL, "unknown stage " + state.getStage(), "");
    }
  }

  private void afterTrack(TrackResult r) {
    if (r == null) {
      return; // interrupted: the main loop handles the close or reassignment
    }
    String reason = r.getReason();
    switch (r.getOutcome()) {
      case ROUTE_STP -> enter(STAGE_STP, reason, "");
      case ROUTE_INVESTIGATION -> enter(STAGE_INVESTIGATION, reason, "");
      case ROUTE_SIU -> enter(STAGE_SIU, reason, "");
      case RETRIAGE -> retriage(reason);
      case COMPLETED -> enter(STAGE_CLOSED, reason, "");
      default -> enter(STAGE_MANUAL, reason.isEmpty() ? "track outcome " + r.getOutcome() : reason, "");
    }
  }

  private void retriage(String reason) {
    if (state.getPolicy().getPilotStpOnly()) {
      enter(STAGE_MANUAL, reason + " (pilot scope: no triage track; an adjuster decides)", "");
      return;
    }
    state.setRetriageCount(state.getRetriageCount() + 1);
    int limit = state.getPolicy().getRetriageLimit() > 0 ? state.getPolicy().getRetriageLimit() : 3;
    if (state.getRetriageCount() > limit) {
      enter(STAGE_ESCALATION, "re-triage limit " + limit + " reached: " + reason, "");
    } else {
      enter(STAGE_TRIAGE, reason, "");
    }
  }

  private void manual() {
    DecisionResult d = desk.decide(DecisionKind.MANUAL_HANDLING,
        List.of("CLOSE", "RETURN_TO_STP", "RETRIAGE", "REFER_SIU"), state.getRouteReason(), 0, "", this::interrupted);
    if (d == null) {
      return;
    }
    switch (d.getChoice()) {
      case "CLOSE" -> closeClaim("closed by adjuster", d.getDecisionId());
      case "RETURN_TO_STP" -> enter(STAGE_STP, "returned to automation by adjuster", d.getDecisionId());
      case "RETRIAGE" -> retriage("re-triage asked by adjuster");
      case "REFER_SIU" -> enter(STAGE_SIU, "referred to SIU by adjuster", d.getDecisionId());
      default -> enter(STAGE_ESCALATION, "manual handling deadline passed", "");
    }
  }

  private void escalation() {
    DecisionResult d = desk.decide(DecisionKind.ESCALATION, List.of("REASSIGN", "CLOSE"),
        state.getRouteReason(), 0, "", this::interrupted);
    if (d == null) {
      return;
    }
    switch (d.getChoice()) {
      case "REASSIGN" -> enter(STAGE_MANUAL, "reassigned by supervisor", d.getDecisionId());
      case "CLOSE" -> closeClaim("closed by supervisor", d.getDecisionId());
      default -> enter(STAGE_ESCALATION, "escalation deadline passed", "");
    }
  }

  private void siu() {
    DecisionResult d = desk.decide(DecisionKind.SIU_REVIEW, List.of("CLEAR", "DENY"),
        state.getRouteReason(), 0, "", this::interrupted);
    if (d == null) {
      return;
    }
    switch (d.getChoice()) {
      case "CLEAR" -> enter(STAGE_TRIAGE, "cleared by SIU", d.getDecisionId());
      case "DENY" -> closeClaim("denied by SIU", d.getDecisionId());
      default -> enter(STAGE_SIU, "SIU deadline passed", "");
    }
  }

  private void reopenReview() {
    DecisionResult d = desk.decide(DecisionKind.REOPEN_REVIEW, List.of("APPROVE", "REJECT"),
        state.getRouteReason(), 0, "", this::interrupted);
    if (d == null) {
      return;
    }
    switch (d.getChoice()) {
      case "APPROVE" -> enter(STAGE_TRIAGE, "reopen approved", d.getDecisionId());
      case "REJECT" -> closeClaim("reopen rejected", d.getDecisionId());
      default -> enter(STAGE_REOPEN_REVIEW, "reopen review deadline passed", "");
    }
  }

  private void closeClaim(String reason, String trigger) {
    int v = state.getStageVersion();
    steps.run("claim.close", () -> claimsPro.closeClaim(CloseClaimRequest.newBuilder()
        .setStep(step(v, "claim.close")).setReason(reason).build()));
    enter(STAGE_CLOSED, reason, trigger);
  }

  // ---------------------------------------------------------------------------------------- tracks

  /** Runs one track visit as a child workflow. Returns null if the claim was closed or reassigned meanwhile. */
  private TrackResult runTrack(Track track, String workflowType, String taskQueue, String name) {
    int v = state.getStageVersion();
    String childId = Names.trackWorkflowId(state.getClaimId(), name, v);
    TrackRequest request = TrackRequest.newBuilder()
        .setClaimId(state.getClaimId()).setStageVersion(v).setTrack(track)
        .setReason(state.getRouteReason()).addAllBufferedEvents(pending).build();
    pending.clear();

    ChildWorkflowStub child = Workflow.newUntypedChildWorkflowStub(workflowType, ChildWorkflowOptions.newBuilder()
        .setWorkflowId(childId)
        .setTaskQueue(taskQueue)
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
        .setCancellationType(ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setTypedSearchAttributes(SearchAttributes.newBuilder()
            .set(SearchAttrs.CLAIM_ID, state.getClaimId()).set(SearchAttrs.TRACK, name).build())
        .build());
    List<Promise<TrackResult>> result = new ArrayList<>(1);
    CancellationScope scope = Workflow.newCancellationScope(() -> result.add(child.executeAsync(TrackResult.class, request)));
    scope.run();
    child.getExecution().get(); // started
    activeChildId = childId;
    if (track == Track.TRACK_STP) {
      state.setLastStpStage(v); // the payment of this visit is claim:{id}:payment:{claimId}-v{v}-1
    }
    recorder.row(v, "track.start:" + name).ref("workflow_id", childId).write();

    try {
      while (true) {
        Workflow.await(() -> result.get(0).isCompleted() || interrupted() || !toForward.isEmpty());
        forward(childId);
        if (result.get(0).isCompleted()) {
          break;
        }
        if (interrupted()) {
          // Cancellation request, never termination: the child withdraws its tasks and writes its last row.
          scope.cancel(closedTrigger != null ? "claim closed in ClaimsPro" : "claim reassigned in ClaimsPro");
          try {
            result.get(0).get();
          } catch (TemporalFailure ignored) {
            // cancelled as requested
          }
          recorder.row(v, "track.cancelled:" + name).outcome("CANCELLED").write();
          return null;
        }
      }
      TrackResult r = result.get(0).get();
      recorder.row(v, "track.result:" + name).outcome(r.getOutcome().name()).reason(r.getReason())
          .ref("flow", r.getFlowRef()).write();
      return r;
    } catch (TemporalFailure failed) {
      log.warn("Track {} failed: {}", childId, failed.getMessage());
      recorder.row(v, "track.failed:" + name).outcome("FAILED").reason(failed.getMessage()).write();
      return TrackResult.newBuilder().setOutcome(TrackOutcome.ROUTE_MANUAL).setReason(name + " track failed").build();
    } finally {
      activeChildId = null;
    }
  }

  private void forward(String childId) {
    while (!toForward.isEmpty()) {
      ClaimEvent e = toForward.poll();
      try {
        Workflow.newUntypedExternalWorkflowStub(childId).signal(Names.SIGNAL_CLAIM_EVENT, e);
      } catch (TemporalFailure childGone) {
        hold(e); // the next track re-reads ClaimsPro anyway
      }
    }
  }

  private boolean interrupted() {
    return closedTrigger != null || reassignedTrigger != null;
  }

  // ----------------------------------------------------------------------------------- bookkeeping

  private void enter(Stage stage, String reason, String trigger) {
    if (stage == STAGE_TRIAGE && state.getPolicy().getPilotStpOnly()) {
      stage = STAGE_STP; // pilot scope: Claims Lifecycle -> STP only
      reason = reason + " (pilot scope: straight to STP)";
    }
    int v = state.getStageVersion() + 1;
    projection.beginStage(StageChange.newBuilder().setStep(step(v, "stage.begin")).setStage(stage).build());
    state.setStage(stage).setStageVersion(v).setRouteReason(reason == null ? "" : reason);
    Workflow.upsertTypedSearchAttributes(SearchAttrs.STAGE.valueSet(stage.name().replace("STAGE_", "")));
    recorder.row(v, "stage:" + stage.name()).outcome(stage.name()).reason(reason).trigger(trigger).write();
  }

  /**
   * The claim is closed: check its business record, then complete. A reopen that arrives meanwhile (Signal-With-Start
   * reaches this run while it is still open) is reviewed in this run instead of being lost.
   */
  private boolean reopenedWhileClosing() {
    int v = state.getStageVersion();
    try {
      steps.run("record.verify", () -> verifier.verifyRecord(RecordCheck.newBuilder()
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

  /** Forwards each payment cancellation to the payment workflow of the latest STP visit, if one is running. */
  private void forwardPaymentCancels() {
    while (true) {
      Workflow.await(() -> !paymentCancels.isEmpty());
      String why = paymentCancels.poll();
      int stp = state.getLastStpStage();
      String step = "payment.cancel." + (++paymentCancelCount);
      if (stp == 0) {
        recorder.row(state.getStageVersion(), step).outcome("NO_PAYMENT").reason(why).write();
        continue;
      }
      String paymentId = Names.paymentWorkflowId(state.getClaimId(), Names.paymentKey(state.getClaimId(), stp, 1));
      try {
        Workflow.newUntypedExternalWorkflowStub(paymentId).signal(Names.SIGNAL_CANCEL_PAYMENT,
            CancelPayment.newBuilder().setRequestedBy("ClaimsPro").setReason(why).build());
        recorder.row(state.getStageVersion(), step).outcome("FORWARDED").reason(why).ref("workflow_id", paymentId).write();
      } catch (TemporalFailure noRunningPayment) {
        recorder.row(state.getStageVersion(), step).outcome("NO_RUNNING_PAYMENT").reason(why).write();
      }
    }
  }

  private void continueAsNew() {
    Workflow.await(Workflow::isEveryHandlerFinished);
    recorder.closing(state.getStageVersion(), "CONTINUED_AS_NEW");
    List<String> lastSeen = new ArrayList<>(seen);
    ClaimState carried = state.clone()
        .addAllSeenEventIds(lastSeen.subList(Math.max(0, lastSeen.size() - MAX_SEEN_EVENTS), lastSeen.size()))
        .addAllPendingEvents(pending).addAllPayments(payments).build();
    Workflow.continueAsNew(ClaimStart.newBuilder().setClaimId(state.getClaimId()).setCarried(carried).build());
  }

  private void hold(ClaimEvent e) {
    if (pending.size() >= MAX_PENDING_EVENTS) {
      pending.remove(0);
    }
    pending.add(e);
  }

  private ClaimRef ref() {
    return ClaimRef.newBuilder().setClaimId(state.getClaimId()).build();
  }

  private StepRef step(int v, String name) {
    return StepRef.newBuilder().setClaimId(state.getClaimId()).setStageVersion(v).setStep("lifecycle." + name).build();
  }

  // -------------------------------------------------------------------------------------- messages

  @Override
  public void claimEvent(ClaimEvent e) {
    if (!seen.add(e.getEventId())) {
      return; // duplicate
    }
    if (seen.size() > MAX_SEEN_EVENTS * 2) {
      seen.remove(seen.iterator().next());
    }
    switch (e.getType()) {
      case CLAIM_CREATED -> { } // the start event of this run, delivered by Signal-With-Start
      case CLAIM_REOPENED -> {
        if (state != null && state.getStage() == STAGE_CLOSED) {
          reopenTrigger = e.getEventId(); // reopened while this run is closing
        }
      }
      case PAYMENT_CANCEL_REQUESTED -> paymentCancels.add("payment cancelled in ClaimsPro (event " + e.getEventId() + ")");
      case CLAIM_CLOSED -> {
        closedTrigger = e.getEventId();
        paymentCancels.add("claim closed in ClaimsPro (event " + e.getEventId() + ")");
      }
      case CLAIM_REASSIGNED -> reassignedTrigger = e.getEventId();
      default -> {
        if (activeChildId != null) {
          toForward.add(e);
        } else {
          hold(e);
        }
      }
    }
  }

  @Override
  public void paymentResult(PaymentResult result) {
    payments.add(result);
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
