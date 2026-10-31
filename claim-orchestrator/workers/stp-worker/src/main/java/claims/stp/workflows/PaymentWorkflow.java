package claims.stp.workflows;

import claims.contracts.v1.CancelPayment;
import claims.contracts.v1.Decision;
import claims.contracts.v1.DecisionResult;
import claims.contracts.v1.PaymentRequest;
import claims.contracts.v1.PaymentResult;
import claims.runtime.Names;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** One payment (claim:{claimId}:payment:{paymentKey}). Keeps running if STP stops; never stops halfway. */
@WorkflowInterface
public interface PaymentWorkflow {
  @WorkflowMethod(name = Names.PAYMENT)
  PaymentResult run(PaymentRequest request);

  /** Honoured until the payment closes. */
  @SignalMethod(name = Names.SIGNAL_CANCEL_PAYMENT)
  void cancelPayment(CancelPayment cancel);

  @UpdateMethod(name = Names.UPDATE_DECIDE)
  DecisionResult decide(Decision decision);

  @UpdateValidatorMethod(updateName = Names.UPDATE_DECIDE)
  void validateDecide(Decision decision);
}
