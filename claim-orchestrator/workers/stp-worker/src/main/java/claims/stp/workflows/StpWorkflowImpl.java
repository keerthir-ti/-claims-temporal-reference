package claims.stp.workflows;

import claims.contracts.v1.AppraisalResult;
import claims.contracts.v1.ClaimEvent;
import claims.contracts.v1.ClaimEventType;
import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.CloseClaimRequest;
import claims.contracts.v1.ContactResult;
import claims.contracts.v1.CoverageResult;
import claims.contracts.v1.Decision;
import claims.contracts.v1.DecisionKind;
import claims.contracts.v1.DecisionPolicy;
import claims.contracts.v1.DecisionResult;
import claims.contracts.v1.DispatchResult;
import claims.contracts.v1.EstimateResult;
import claims.contracts.v1.FlowDefinition;
import claims.contracts.v1.PaymentRequest;
import claims.contracts.v1.PaymentResult;
import claims.contracts.v1.PaymentStatus;
import claims.contracts.v1.PolicyRequest;
import claims.contracts.v1.ReserveRequest;
import claims.contracts.v1.ReserveResult;
import claims.contracts.v1.StepRef;
import claims.contracts.v1.StepRequest;
import claims.contracts.v1.TrackOutcome;
import claims.contracts.v1.TrackRequest;
import claims.contracts.v1.TrackResult;
import claims.runtime.Names;
import claims.runtime.SearchAttrs;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.workflow.DecisionDesk;
import claims.runtime.workflow.Recorder;
import claims.runtime.workflow.Steps;
import claims.runtime.workflow.Stubs;
import claims.stp.activities.CommHubActivities;
import claims.stp.activities.EClaimsActivities;
import claims.stp.activities.FlowActivities;
import claims.stp.activities.PolicyGateActivities;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.SearchAttributes;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The STP steps for one visit, in the order of the claim's flow definition. On a claim change event it
 * re-reads ClaimsPro; a material change ends the visit with RETRIAGE once the in-flight step finishes.
 * Cancelled by the parent: withdraws its open tasks and writes its closing row. A running payment
 * continues regardless.
 */
public class StpWorkflowImpl implements StpWorkflow {
  private boolean estimateEvent;
  private boolean recheck;
  private DecisionDesk desk;

  private TrackRequest request;
  private Recorder recorder;
  private Steps steps;
  private ClaimsProActivities claimsPro;
  private CommHubActivities commHub;
  private PolicyGateActivities policyGate;
  private EClaimsActivities eClaims;
  private FlowActivities flows;
  private long baselineVersion;
  private String flowRef = "";

  @Override
  public TrackResult run(TrackRequest request) {
    this.request = request;
    recorder = new Recorder(request.getClaimId(), Names.STP);
    desk = new DecisionDesk(request.getClaimId(), request::getStageVersion, recorder);
    steps = new Steps(desk);
    request.getBufferedEventsList().forEach(this::claimEvent);
    try {
      return runFlow();
    } catch (TemporalFailure f) {
      if (CancellationScope.current().isCancelRequested()) {
        Workflow.newDetachedCancellationScope(() -> {
          desk.withdrawAll();
          recorder.closing(v(), "CANCELLED");
        }).run();
        throw f;
      }
      if (f instanceof ActivityFailure af && Steps.isBusiness(af)) {
        return finish(TrackOutcome.ROUTE_MANUAL, Steps.businessType(af) + ": " + af.getCause().getMessage());
      }
      throw f;
    } catch (Steps.Stopped stopped) {
      return finish(TrackOutcome.ROUTE_MANUAL, stopped.getMessage());
    }
  }

  private TrackResult runFlow() {
    DecisionPolicy policy = Stubs.policy().load(PolicyRequest.newBuilder().setClaimId(claimId()).build());
    desk.setPolicy(policy);
    Duration window = Stubs.retryWindow(policy);
    claimsPro = Stubs.claimsPro(window);
    commHub = Workflow.newActivityStub(CommHubActivities.class, Stubs.options(Names.STP_QUEUE, Duration.ofSeconds(30), window));
    policyGate = Workflow.newActivityStub(PolicyGateActivities.class, Stubs.options(Names.STP_QUEUE, Duration.ofSeconds(30), window));
    eClaims = Workflow.newActivityStub(EClaimsActivities.class, Stubs.options(Names.STP_QUEUE, Duration.ofSeconds(30), window));
    flows = Workflow.newActivityStub(FlowActivities.class, Stubs.options(Names.STP_QUEUE, Duration.ofSeconds(10), window));

    ClaimSnapshot claim = steps.run("stp.read", () -> claimsPro.readClaim(ref()));
    baselineVersion = claim.getChangeVersion();
    FlowDefinition flow = steps.run("stp.flow", () -> flows.resolve(claim));
    flowRef = flow.getFlowId() + "@" + flow.getVersion() + "#" + flow.getContentHash();
    recorder.row(v(), "stp.flow").outcome(flowRef).reason(request.getReason()).write();

    for (String step : flow.getStepsList()) {
      if (materialChange()) {
        return finish(TrackOutcome.RETRIAGE, "material change during STP");
      }
      TrackResult early = switch (step) {
        case "CONTACT_CUSTOMER" -> contactCustomer();
        case "VERIFY_COVERAGE" -> verifyCoverage();
        case "OPEN_RESERVE" -> openReserve(claim);
        case "DISPATCH_AND_APPRAISAL" -> dispatchAndAppraisal();
        case "AWAIT_ESTIMATE" -> awaitEstimate(flow);
        case "PAY" -> pay(claim);
        case "CLOSE_CLAIM" -> closeClaim();
        default -> finish(TrackOutcome.ROUTE_MANUAL, "unknown step " + step);
      };
      if (early != null) {
        return early;
      }
    }
    return finish(TrackOutcome.COMPLETED, "STP completed");
  }

  // ------------------------------------------------------------------------------------------ steps

  private TrackResult contactCustomer() {
    for (int attempt = 1; ; attempt++) {
      String name = "stp.contact." + attempt;
      ContactResult r = steps.run(name, () -> commHub.contactCustomer(stepRequest(name)));
      recorder.row(v(), name).outcome(r.getReached() ? "REACHED" : "NOT_REACHED").write();
      if (r.getReached()) {
        return null;
      }
      DecisionResult d = decide(DecisionKind.CUSTOMER_NOT_REACHED, List.of("RETRY_CONTACT", "CONTINUE", "MANUAL"),
          "customer not reached after attempt " + attempt, 0, "");
      if (d == null) {
        return retriageOrNull();
      }
      switch (d.getChoice()) {
        case "CONTINUE" -> { return null; }
        case "RETRY_CONTACT" -> { }
        default -> { return finish(TrackOutcome.ROUTE_MANUAL, "customer not reached"); }
      }
    }
  }

  private TrackResult verifyCoverage() {
    CoverageResult r = steps.run("stp.coverage", () -> policyGate.verifyCoverage(stepRequest("stp.coverage")));
    recorder.row(v(), "stp.coverage").outcome(r.getCovered() ? "COVERED" : "NOT_CONFIRMED").reason(r.getReason()).write();
    // A denial is always made by a person.
    return r.getCovered() ? null : finish(TrackOutcome.ROUTE_MANUAL, "coverage not confirmed: " + r.getReason());
  }

  private TrackResult openReserve(ClaimSnapshot claim) {
    ReserveResult r = steps.run("stp.reserve", () -> claimsPro.openReserve(ReserveRequest.newBuilder()
        .setStep(step("stp.reserve")).setAmountCents(claim.getClaimedAmountCents()).build()));
    recorder.row(v(), "stp.reserve").outcome("OPENED").ref("reserve_id", r.getReserveId()).write();
    return null;
  }

  private TrackResult dispatchAndAppraisal() {
    AppraisalResult appraisal = null;
    for (int attempt = 1; ; attempt++) {
      String name = "stp.dispatch." + attempt;
      boolean first = attempt == 1;
      List<Object> both = steps.run(name, () -> {
        Promise<DispatchResult> dispatch = Async.function(eClaims::dispatchVendor, stepRequest(name));
        Promise<AppraisalResult> assign = first
            ? Async.function(eClaims::assignAppraisal, stepRequest("stp.appraisal"))
            : Workflow.newPromise(AppraisalResult.getDefaultInstance());
        List<Object> out = new ArrayList<>();
        out.add(dispatch.get());
        out.add(assign.get());
        return out;
      });
      DispatchResult dispatch = (DispatchResult) both.get(0);
      if (first) {
        appraisal = (AppraisalResult) both.get(1);
        recorder.row(v(), "stp.appraisal").outcome("ASSIGNED").ref("appraisal_id", appraisal.getAppraisalId()).write();
      }
      recorder.row(v(), name).outcome(dispatch.getAccepted() ? "ACCEPTED" : "NO_VENDOR")
          .ref("vendor_id", dispatch.getVendorId()).write();
      if (dispatch.getAccepted()) {
        return null;
      }
      DecisionResult d = decide(DecisionKind.NO_VENDOR, List.of("RETRY_DISPATCH", "MANUAL"), "no vendor accepted", 0, "");
      if (d == null) {
        return retriageOrNull();
      }
      if (!"RETRY_DISPATCH".equals(d.getChoice())) {
        return finish(TrackOutcome.ROUTE_MANUAL, "no vendor accepted");
      }
    }
  }

  private TrackResult awaitEstimate(FlowDefinition flow) {
    Duration recheckEvery = Duration.ofSeconds(Math.max(5, flow.getEstimateRecheckSeconds()));
    EstimateResult estimate;
    String via = "read";
    while (true) {
      estimate = steps.run("stp.estimate", () -> eClaims.getEstimate(ref()));
      if (estimate.getAvailable()) {
        break;
      }
      // Wait for the event; the timer re-reads in case the event is lost.
      boolean signalled = Workflow.await(recheckEvery, () -> estimateEvent || recheck);
      via = estimateEvent ? "event" : (signalled ? "change" : "timer re-read");
      estimateEvent = false;
      if (materialChange()) {
        return finish(TrackOutcome.RETRIAGE, "material change while waiting for the estimate");
      }
    }
    recorder.row(v(), "stp.estimate").outcome(Long.toString(estimate.getAmountCents())).reason("received by " + via).write();
    this.estimateCents = estimate.getAmountCents();
    if (flow.getEstimateReviewCents() > 0 && estimate.getAmountCents() > flow.getEstimateReviewCents()) {
      DecisionResult d = decide(DecisionKind.ESTIMATE_REVIEW, List.of("APPROVE", "MANUAL"),
          "estimate " + estimate.getAmountCents() + " above review threshold " + flow.getEstimateReviewCents(), 0, "");
      if (d == null) {
        return retriageOrNull();
      }
      if (!"APPROVE".equals(d.getChoice())) {
        return finish(TrackOutcome.ROUTE_MANUAL, "estimate sent for manual handling");
      }
    }
    return null;
  }

  private long estimateCents;

  private TrackResult pay(ClaimSnapshot claim) {
    String key = Names.paymentKey(claimId(), v(), 1);
    PaymentRequest req = PaymentRequest.newBuilder()
        .setClaimId(claimId()).setStageVersion(v()).setPaymentKey(key)
        .setAmountCents(estimateCents > 0 ? estimateCents : claim.getClaimedAmountCents())
        .setPayeeRef("vendor-or-claimant:" + claimId()).setRequestedBy(claim.getAdjusterId())
        .setStpWorkflowId(Workflow.getInfo().getWorkflowId()).build();
    ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
        .setWorkflowId(Names.paymentWorkflowId(claimId(), key))
        .setTaskQueue(Names.STP_QUEUE)
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setTypedSearchAttributes(SearchAttributes.newBuilder().set(SearchAttrs.CLAIM_ID, claimId()).set(SearchAttrs.TRACK, "payment").build())
        .build();
    // Started outside STP's cancellation scope: cancelling STP never cancels the payment.
    List<Promise<PaymentResult>> result = new ArrayList<>(1);
    Workflow.newDetachedCancellationScope(() ->
        result.add(Workflow.newUntypedChildWorkflowStub(Names.PAYMENT, options).executeAsync(PaymentResult.class, req))).run();
    recorder.row(v(), "stp.payment.start").ref("payment_key", key).write();
    PaymentResult paid = result.get(0).get(); // the in-flight step finishes even on a material change
    recorder.row(v(), "stp.payment").outcome(paid.getStatus().name()).ref("payment_id", paid.getPaymentId())
        .actor(paid.getApprovedBy()).reason(paid.getReason()).write();
    return paid.getStatus() == PaymentStatus.PAYMENT_CLOSED ? null
        : finish(TrackOutcome.ROUTE_MANUAL, "payment " + paid.getStatus().name() + ": " + paid.getReason());
  }

  private TrackResult closeClaim() {
    steps.run("stp.close", () -> claimsPro.closeClaim(CloseClaimRequest.newBuilder()
        .setStep(step("stp.close")).setReason("closed by STP").build()));
    return finish(TrackOutcome.COMPLETED, "closed by STP");
  }

  // ---------------------------------------------------------------------------------------- helpers

  private DecisionResult decide(DecisionKind kind, List<String> choices, String context, long authority, String requestedBy) {
    return desk.decide(kind, choices, context, authority, requestedBy, () -> recheck);
  }

  /** A decision was interrupted by a change event: re-triage if material, otherwise carry on. */
  private TrackResult retriageOrNull() {
    return materialChange() ? finish(TrackOutcome.RETRIAGE, "material change during a decision") : null;
  }

  private boolean materialChange() {
    if (recheck) {
      recheck = false;
      ClaimSnapshot now = steps.run("stp.reread", () -> claimsPro.readClaim(ref()));
      if (now.getChangeVersion() != baselineVersion) {
        recorder.row(v(), "stp.material_change").outcome("RETRIAGE")
            .reason("change version " + baselineVersion + " -> " + now.getChangeVersion()).write();
        baselineVersion = now.getChangeVersion();
        return true;
      }
    }
    return false;
  }

  private TrackResult finish(TrackOutcome outcome, String reason) {
    recorder.closing(v(), outcome.name());
    return TrackResult.newBuilder().setOutcome(outcome).setReason(reason).setFlowRef(flowRef).build();
  }

  private String claimId() {
    return request.getClaimId();
  }

  private int v() {
    return request.getStageVersion();
  }

  private ClaimRef ref() {
    return ClaimRef.newBuilder().setClaimId(claimId()).build();
  }

  private StepRef step(String name) {
    return StepRef.newBuilder().setClaimId(claimId()).setStageVersion(v()).setStep(name).build();
  }

  private StepRequest stepRequest(String name) {
    return StepRequest.newBuilder().setStep(step(name)).build();
  }

  // --------------------------------------------------------------------------------------- messages

  @Override
  public void claimEvent(ClaimEvent event) {
    if (event.getType() == ClaimEventType.ESTIMATE_RECEIVED) {
      estimateEvent = true;
    } else {
      recheck = true; // re-read ClaimsPro before acting on it
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
