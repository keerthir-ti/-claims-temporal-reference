package claims.lifecycle.workflows;

import claims.contracts.AppraisalResult;
import claims.contracts.ClaimSnapshot;
import claims.contracts.CloseClaimRequest;
import claims.contracts.ContactResult;
import claims.contracts.CoverageResult;
import claims.contracts.CreatePaymentRequest;
import claims.contracts.DecisionKind;
import claims.contracts.DecisionResult;
import claims.contracts.DispatchResult;
import claims.contracts.EstimateResult;
import claims.contracts.FlowDefinition;
import claims.contracts.FlowStage;
import claims.contracts.PaymentKeyRef;
import claims.contracts.PaymentRecord;
import claims.contracts.ReserveRequest;
import claims.contracts.ReserveResult;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;
import claims.runtime.Names;
import claims.runtime.workflow.StepRunner;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * STP, inside the claim workflow: runs the claim's flow definition. Stages run in order; the steps of a stage
 * run in parallel (activities on claims-stp and claimspro-activities). On a claim change the module re-reads
 * ClaimsPro; a material change cancels the running stage at once, unless it moves money, and returns RETRIAGE.
 * The payment runs outside every cancellation scope and is never stopped halfway.
 */
final class StpModule {
  /** Ceiling on straight-through payment authority, in code: configuration can lower it, never raise it. */
  static final long STP_AUTHORITY_CEILING_CENTS = 2_500_000L; // 25,000.00
  static final String PAY_BALANCE = "pay-balance";

  private record StepOutcome(boolean ok, String reason) {
    static final StepOutcome OK = new StepOutcome(true, "");

    static StepOutcome handoff(String reason) {
      return new StepOutcome(false, reason);
    }
  }

  private final ClaimContext c;
  private FlowDefinition flow;
  private ClaimSnapshot baseline;
  private String flowRef = "";
  private String pendingRetriage;
  private long estimateCents;
  private boolean paid;

  StpModule(ClaimContext c) {
    this.c = c;
  }

  TrackResult run(String reason) {
    c.claimChanged = false;
    c.estimateReceived = false;
    try {
      baseline = c.steps.run("stp.read", () -> c.claimsPro.readClaim(c.ref()));
      flow = c.steps.run("stp.flow", () -> c.flows.resolve(baseline));
      flowRef = flow.getFlowId() + "@" + flow.getVersion() + "#" + flow.getContentHash();
      c.recorder.row(c.stage(), "stp.flow").outcome(flowRef).reason(reason).write();
      for (FlowStage stage : flow.getStagesList()) {
        if (paid && pendingRetriage != null) {
          // Money has moved: STP closes the claim; the change is on record and a reopen brings it to an adjuster.
          c.recorder.row(c.stage(), "stp.material_change.after_payment").outcome("AFTER_PAYMENT").reason(pendingRetriage).write();
          pendingRetriage = null;
        }
        String material = materialChange();
        if (material != null || pendingRetriage != null) {
          return result(TrackOutcome.RETRIAGE, material != null ? material : pendingRetriage);
        }
        TrackResult end = runStage(stage);
        if (end != null) {
          return end;
        }
      }
      return result(TrackOutcome.COMPLETED, "STP completed");
    } catch (ActivityFailure f) {
      if (StepRunner.isBusiness(f)) {
        return result(TrackOutcome.ROUTE_MANUAL, StepRunner.businessType(f) + ": " + f.getCause().getMessage());
      }
      throw f;
    } catch (StepRunner.Stopped stopped) {
      return result(TrackOutcome.ROUTE_MANUAL, stopped.getMessage());
    }
  }

  private TrackResult runStage(FlowStage stage) {
    List<Promise<StepOutcome>> running = new ArrayList<>();
    CancellationScope scope = Workflow.newCancellationScope(() -> {
      for (String step : stage.getStepsList()) {
        running.add(Async.function(this::runStep, step));
      }
    });
    scope.run();
    Promise<Void> all = Promise.allOf(running);
    c.recorder.row(c.stage(), "stp.stage:" + stage.getName()).outcome(String.join(",", stage.getStepsList())).write();
    while (!all.isCompleted()) {
      Workflow.await(() -> all.isCompleted() || c.claimChanged);
      if (!all.isCompleted() && c.claimChanged) {
        String material = materialChange();
        if (material != null) {
          if (stage.getInterruptible()) {
            scope.cancel(material);
            try {
              all.get();
            } catch (TemporalFailure | StepRunner.Stopped cancelled) {
              // cancelled as requested
            }
            c.desk.withdrawAll();
            c.recorder.row(c.stage(), "stp.stage.cancelled:" + stage.getName()).outcome("CANCELLED").reason(material).write();
            return result(TrackOutcome.RETRIAGE, material);
          }
          pendingRetriage = material; // money stage: finish it, then re-triage
        }
      }
    }
    for (Promise<StepOutcome> p : running) {
      if (!p.get().ok()) {
        return result(TrackOutcome.ROUTE_MANUAL, p.get().reason());
      }
    }
    return null;
  }

  private StepOutcome runStep(String step) {
    try {
      return switch (step) {
        case "CONTACT_CUSTOMER" -> contactCustomer();
        case "VERIFY_COVERAGE" -> verifyCoverage();
        case "OPEN_RESERVE" -> openReserve();
        case "ASSIGN_APPRAISAL" -> assignAppraisal();
        case "DISPATCH" -> dispatch();
        case "AWAIT_ESTIMATE" -> awaitEstimate();
        case "PAY" -> pay();
        case "CLOSE_CLAIM" -> closeClaim();
        default -> StepOutcome.handoff("unknown step " + step);
      };
    } catch (ActivityFailure f) {
      if (StepRunner.isBusiness(f)) {
        return StepOutcome.handoff(step + ": " + StepRunner.businessType(f) + " " + f.getCause().getMessage());
      }
      throw f;
    } catch (StepRunner.Stopped stopped) {
      return StepOutcome.handoff(stopped.getMessage());
    }
  }

  // ------------------------------------------------------------------------------------------ steps

  private StepOutcome contactCustomer() {
    for (int attempt = 1; ; attempt++) {
      String name = "stp.contact." + attempt;
      ContactResult r = c.steps.run(name, () -> c.commHub.contactCustomer(c.stepRequest(name)));
      c.recorder.row(c.stage(), name).outcome(r.getReached() ? "REACHED" : "NOT_REACHED").write();
      if (r.getReached()) {
        return StepOutcome.OK;
      }
      DecisionResult d = c.desk.decide(DecisionKind.CUSTOMER_NOT_REACHED, List.of("RETRY_CONTACT", "CONTINUE", "MANUAL"),
          "customer not reached after attempt " + attempt, 0, "", () -> false);
      if (d == null || !List.of("RETRY_CONTACT", "CONTINUE").contains(d.getChoice())) {
        return StepOutcome.handoff("customer not reached");
      }
      if ("CONTINUE".equals(d.getChoice())) {
        return StepOutcome.OK;
      }
    }
  }

  private StepOutcome verifyCoverage() {
    CoverageResult r = c.steps.run("stp.coverage", () -> c.policyGate.verifyCoverage(c.stepRequest("stp.coverage")));
    c.recorder.row(c.stage(), "stp.coverage").outcome(r.getCovered() ? "COVERED" : "NOT_CONFIRMED").reason(r.getReason()).write();
    return r.getCovered() ? StepOutcome.OK : StepOutcome.handoff("coverage not confirmed: " + r.getReason());
  }

  private StepOutcome openReserve() {
    ReserveResult r = c.steps.run("stp.reserve", () -> c.claimsPro.openReserve(ReserveRequest.newBuilder()
        .setStep(c.step("stp.reserve")).setAmountCents(baseline.getClaimedAmountCents()).build()));
    c.recorder.row(c.stage(), "stp.reserve").outcome("OPENED").ref("reserve_id", r.getReserveId()).write();
    return StepOutcome.OK;
  }

  private StepOutcome assignAppraisal() {
    AppraisalResult r = c.steps.run("stp.appraisal", () -> c.eClaims.assignAppraisal(c.stepRequest("stp.appraisal")));
    c.recorder.row(c.stage(), "stp.appraisal").outcome("ASSIGNED").ref("appraisal_id", r.getAppraisalId()).write();
    return StepOutcome.OK;
  }

  /** Vendors one by one; after each round of declines an adjuster decides. */
  private StepOutcome dispatch() {
    int perRound = flow.getVendorsPerRound() > 0 ? flow.getVendorsPerRound() : 3;
    for (int attempt = 1; ; attempt++) {
      String name = "stp.dispatch." + attempt;
      DispatchResult offer = c.steps.run(name, () -> c.eClaims.dispatchVendor(c.stepRequest(name)));
      c.recorder.row(c.stage(), name).outcome(offer.getAccepted() ? "ACCEPTED" : "DECLINED").ref("vendor_id", offer.getVendorId()).write();
      if (offer.getAccepted()) {
        return StepOutcome.OK;
      }
      if (attempt % perRound == 0) {
        DecisionResult d = c.desk.decide(DecisionKind.NO_VENDOR, List.of("RETRY_DISPATCH", "MANUAL"),
            attempt + " vendors declined", 0, "", () -> false);
        if (d == null || !"RETRY_DISPATCH".equals(d.getChoice())) {
          return StepOutcome.handoff("no vendor accepted");
        }
      }
    }
  }

  private StepOutcome awaitEstimate() {
    Duration recheckEvery = Duration.ofSeconds(Math.max(5, flow.getEstimateRecheckSeconds()));
    String via = "read";
    EstimateResult estimate;
    while (true) {
      estimate = c.steps.run("stp.estimate", () -> c.eClaims.getEstimate(c.ref()));
      if (estimate.getAvailable()) {
        break;
      }
      boolean signalled = Workflow.await(recheckEvery, () -> c.estimateReceived);
      via = signalled ? "event" : "timer re-read";
      c.estimateReceived = false;
    }
    estimateCents = estimate.getAmountCents();
    c.recorder.row(c.stage(), "stp.estimate").outcome(Long.toString(estimateCents)).reason("received by " + via).write();
    if (flow.getEstimateReviewCents() > 0 && estimateCents > flow.getEstimateReviewCents()) {
      DecisionResult d = c.desk.decide(DecisionKind.ESTIMATE_REVIEW, List.of("APPROVE", "MANUAL"),
          "estimate " + estimateCents + " above review threshold " + flow.getEstimateReviewCents(), 0, "", () -> false);
      if (d == null || !"APPROVE".equals(d.getChoice())) {
        return StepOutcome.handoff("estimate sent for manual handling");
      }
    }
    return StepOutcome.OK;
  }

  /** The payment region: started outside every cancellation scope, so a cancelled STP never stops it. */
  /**
   * Pays the balance: the estimate (or claimed amount) less what ClaimsPro already shows as paid, so a later STP visit
   * of the same claim (after a re-triage or a return from manual handling) never pays twice.
   */
  private StepOutcome pay() {
    String key = Names.paymentKey(c.claimId, c.stage(), 1);
    long target = estimateCents > 0 ? estimateCents : baseline.getClaimedAmountCents();
    long alreadyPaid = Workflow.getVersion(PAY_BALANCE, Workflow.DEFAULT_VERSION, 1) >= 1
        ? c.steps.run("stp.paid", () -> c.claimsPro.readClaim(c.ref())).getPaidCents() : 0;
    long amount = target - alreadyPaid;
    if (amount <= 0) {
      c.recorder.row(c.stage(), "stp.payment").outcome("ALREADY_PAID").reason("paid to date " + alreadyPaid).write();
      paid = true;
      return StepOutcome.OK;
    }
    List<Promise<String>> result = new ArrayList<>(1);
    Workflow.newDetachedCancellationScope(() -> result.add(Async.function(() -> payment(key, amount)))).run();
    c.payments.add(result.get(0).thenApply(r -> null));
    String outcome = result.get(0).get();
    paid = "PAID".equals(outcome);
    return paid ? StepOutcome.OK : StepOutcome.handoff("payment " + outcome);
  }

  private String payment(String key, long amount) {
    int v = c.stage();
    c.cancelPaymentRequested = false;
    c.recorder.row(v, "payment.requested:" + key).outcome(Long.toString(amount)).actor(baseline.getAdjusterId()).write();
    String approvedBy = "stp-authority";
    long authority = Math.min(c.policy.getStpPaymentAuthorityCents(), STP_AUTHORITY_CEILING_CENTS);
    if (amount > authority) {
      while (true) {
        DecisionResult d = c.desk.decide(DecisionKind.PAYMENT_APPROVAL, List.of("APPROVE", "REJECT"),
            "payment " + amount + " above STP authority " + authority,
            amount, baseline.getAdjusterId(), () -> c.cancelPaymentRequested);
        if (d == null) {
          return finishPayment(v, key, "CANCELLED", "", "cancelled by " + c.cancelPaymentBy);
        }
        if ("APPROVE".equals(d.getChoice())) {
          approvedBy = d.getDecidedBy();
          break;
        }
        if ("REJECT".equals(d.getChoice())) {
          return finishPayment(v, key, "REJECTED", "", d.getReason());
        }
      }
    }
    if (c.cancelPaymentRequested) {
      return finishPayment(v, key, "CANCELLED", "", "cancelled by " + c.cancelPaymentBy);
    }
    PaymentKeyRef ref = PaymentKeyRef.newBuilder().setStep(c.step("payment.key:" + key)).setPaymentKey(key).build();
    PaymentRecord created;
    try {
      created = c.steps.run("payment.create", () -> c.claimsPro.createPayment(CreatePaymentRequest.newBuilder()
          .setStep(c.step("payment.create:" + key)).setPaymentKey(key).setAmountCents(amount)
          .setPayeeRef("vendor-or-claimant:" + c.claimId).build()));
    } catch (ActivityFailure f) {
      if (StepRunner.isBusiness(f)) {
        return finishPayment(v, key, "REJECTED", "", StepRunner.businessType(f));
      }
      throw f;
    } catch (StepRunner.Stopped stopped) {
      return finishPayment(v, key, "REJECTED", "", stopped.getMessage());
    }
    c.recorder.row(v, "payment.created:" + key).outcome(created.getStatus()).ref("payment_id", created.getPaymentId()).write();
    if (c.cancelPaymentRequested) {
      c.claimsProUntilDone.cancelPayment(ref);
      return finishPayment(v, key, "CANCELLED", created.getPaymentId(), "cancelled by " + c.cancelPaymentBy);
    }
    PaymentRecord closed = c.claimsProUntilDone.closePayment(ref); // once created, always closed
    c.recorder.row(v, "payment.approved:" + key).actor(approvedBy).outcome("APPROVED").write();
    return finishPayment(v, key, "PAID", closed.getPaymentId(), "");
  }

  private String finishPayment(int v, String key, String outcome, String paymentId, String reason) {
    c.recorder.row(v, "payment.result:" + key).outcome(outcome).reason(reason).ref("payment_id", paymentId).write();
    return outcome;
  }

  private StepOutcome closeClaim() {
    c.steps.run("stp.close", () -> c.claimsPro.closeClaim(CloseClaimRequest.newBuilder()
        .setStep(c.step("stp.close")).setReason("closed by STP").build()));
    c.recorder.row(c.stage(), "stp.close").outcome("CLOSED").write();
    return StepOutcome.OK;
  }

  // ---------------------------------------------------------------------------------------- helpers

  private String materialChange() {
    if (!c.claimChanged) {
      return null;
    }
    c.claimChanged = false;
    ClaimSnapshot now = c.steps.run("stp.reread", () -> c.claimsPro.readClaim(c.ref()));
    String reason = MaterialChange.detect(baseline, now);
    baseline = now;
    if (reason == null) {
      return null;
    }
    c.recorder.row(c.stage(), "stp.material_change:" + now.getChangeVersion()).outcome(paid ? "AFTER_PAYMENT" : "RETRIAGE")
        .reason(reason).write();
    return paid ? null : reason;
  }

  private TrackResult result(TrackOutcome outcome, String reason) {
    c.recorder.row(c.stage(), "stp.result").outcome(outcome.name()).reason(reason).write();
    return TrackResult.newBuilder().setOutcome(outcome).setReason(reason).setFlowRef(flowRef).build();
  }
}
