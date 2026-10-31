package claims.stp.workflows;

import claims.contracts.v1.CancelPayment;
import claims.contracts.v1.CreatePaymentRequest;
import claims.contracts.v1.Decision;
import claims.contracts.v1.DecisionKind;
import claims.contracts.v1.DecisionPolicy;
import claims.contracts.v1.DecisionResult;
import claims.contracts.v1.PaymentKeyRef;
import claims.contracts.v1.PaymentRecord;
import claims.contracts.v1.PaymentRequest;
import claims.contracts.v1.PaymentResult;
import claims.contracts.v1.PaymentStatus;
import claims.contracts.v1.PolicyRequest;
import claims.contracts.v1.StepRef;
import claims.runtime.Names;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.workflow.DecisionDesk;
import claims.runtime.workflow.Recorder;
import claims.runtime.workflow.Steps;
import claims.runtime.workflow.Stubs;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.workflow.Workflow;
import java.util.List;

/**
 * Approval above STP authority (a different person with enough authority), then create and close.
 * Create looks up the payment key first, so a rerun never pays twice. Once created, the payment is
 * always closed: close retries without limit. The result goes to the parent by signal.
 */
public class PaymentWorkflowImpl implements PaymentWorkflow {
  private boolean cancelRequested;
  private String cancelledBy = "";
  private DecisionDesk desk;

  private PaymentRequest request;
  private Recorder recorder;

  @Override
  public PaymentResult run(PaymentRequest request) {
    this.request = request;
    recorder = new Recorder(request.getClaimId(), Names.PAYMENT);
    desk = new DecisionDesk(request.getClaimId(), request::getStageVersion, recorder);
    try {
      return pay();
    } catch (ActivityFailure f) {
      // e.g. the claim moved to a newer stage before the payment was created
      if (Steps.isBusiness(f)) {
        return finish(PaymentStatus.PAYMENT_REJECTED, "", Steps.businessType(f) + ": " + f.getCause().getMessage(), "");
      }
      throw f;
    } catch (Steps.Stopped stopped) {
      return finish(PaymentStatus.PAYMENT_REJECTED, "", stopped.getMessage(), "");
    }
  }

  private PaymentResult pay() {
    Steps steps = new Steps(desk);
    DecisionPolicy policy = Stubs.policy().load(PolicyRequest.newBuilder().setClaimId(request.getClaimId()).build());
    desk.setPolicy(policy);
    ClaimsProActivities claimsPro = Stubs.claimsPro(Stubs.retryWindow(policy));
    ClaimsProActivities finishing = Stubs.claimsPro(null);
    recorder.row(v(), "payment.requested").outcome(Long.toString(request.getAmountCents()))
        .actor(request.getRequestedBy()).ref("payment_key", request.getPaymentKey()).write();

    String approvedBy = "stp-authority";
    if (request.getAmountCents() > policy.getStpPaymentAuthorityCents()) {
      while (true) {
        DecisionResult d = desk.decide(DecisionKind.PAYMENT_APPROVAL, List.of("APPROVE", "REJECT"),
            "payment " + request.getAmountCents() + " above STP authority " + policy.getStpPaymentAuthorityCents(),
            request.getAmountCents(), request.getRequestedBy(), () -> cancelRequested);
        if (d == null) {
          return finish(PaymentStatus.PAYMENT_CANCELLED, "", "cancelled by " + cancelledBy, "");
        }
        if ("APPROVE".equals(d.getChoice())) {
          approvedBy = d.getDecidedBy();
          break;
        }
        if ("REJECT".equals(d.getChoice())) {
          return finish(PaymentStatus.PAYMENT_REJECTED, "", "rejected by " + d.getDecidedBy(), "");
        }
      }
    }
    if (cancelRequested) {
      return finish(PaymentStatus.PAYMENT_CANCELLED, "", "cancelled by " + cancelledBy, "");
    }

    PaymentKeyRef key = PaymentKeyRef.newBuilder().setStep(step("payment.key")).setPaymentKey(request.getPaymentKey()).build();
    PaymentRecord created = steps.run("payment.create", () -> claimsPro.createPayment(CreatePaymentRequest.newBuilder()
        .setStep(step("payment.create")).setPaymentKey(request.getPaymentKey())
        .setAmountCents(request.getAmountCents()).setPayeeRef(request.getPayeeRef()).build()));
    recorder.row(v(), "payment.created").outcome(created.getStatus()).ref("payment_id", created.getPaymentId()).write();

    if (cancelRequested) {
      finishing.cancelPayment(key);
      return finish(PaymentStatus.PAYMENT_CANCELLED, created.getPaymentId(), "cancelled by " + cancelledBy, approvedBy);
    }
    PaymentRecord closed = finishing.closePayment(key);
    return finish(PaymentStatus.PAYMENT_CLOSED, closed.getPaymentId(), "", approvedBy);
  }

  private PaymentResult finish(PaymentStatus status, String paymentId, String reason, String approvedBy) {
    PaymentResult result = PaymentResult.newBuilder()
        .setClaimId(request.getClaimId()).setPaymentKey(request.getPaymentKey()).setStatus(status)
        .setPaymentId(paymentId).setApprovedBy(approvedBy).setReason(reason).build();
    recorder.row(v(), "payment.result").outcome(status.name()).actor(approvedBy).reason(reason)
        .ref("payment_id", paymentId).write();
    try {
      Workflow.newUntypedExternalWorkflowStub(Names.claimWorkflowId(request.getClaimId()))
          .signal(Names.SIGNAL_PAYMENT_RESULT, result);
    } catch (TemporalFailure parentClosed) {
      // the claim workflow has completed; the result is in the projection table
    }
    recorder.closing(v(), status.name());
    return result;
  }

  private int v() {
    return request.getStageVersion();
  }

  private StepRef step(String name) {
    return StepRef.newBuilder().setClaimId(request.getClaimId()).setStageVersion(v())
        .setStep(name + ":" + request.getPaymentKey()).build();
  }

  @Override
  public void cancelPayment(CancelPayment cancel) {
    cancelRequested = true;
    cancelledBy = cancel.getRequestedBy();
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
