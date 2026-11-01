package claims.stp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import claims.contracts.CancelPayment;
import claims.contracts.Decision;
import claims.contracts.OpenDecision;
import claims.contracts.PaymentRequest;
import claims.contracts.PaymentResult;
import claims.contracts.PaymentStatus;
import claims.runtime.Names;
import claims.stp.workflows.PaymentWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import org.junit.jupiter.api.Test;

class PaymentWorkflowTest {
  private static PaymentRequest request(long cents) {
    return PaymentRequest.newBuilder().setClaimId("C1").setStageVersion(2).setPaymentKey("C1-v2-1")
        .setAmountCents(cents).setRequestedBy("adj-1").build();
  }

  private static PaymentWorkflow start(StpTestEnv t, PaymentRequest r) {
    PaymentWorkflow wf = t.env.getWorkflowClient().newWorkflowStub(PaymentWorkflow.class,
        WorkflowOptions.newBuilder().setWorkflowId("claim:C1:payment:" + r.getPaymentKey()).setTaskQueue(Names.STP_QUEUE).build());
    WorkflowClient.start(wf::run, r);
    return wf;
  }

  @Test
  void withinAuthorityIsCreatedOnceAndClosed() {
    try (StpTestEnv t = new StpTestEnv()) {
      PaymentWorkflow wf = start(t, request(400_000));
      PaymentResult result = WorkflowStub.fromTyped(wf).getResult(PaymentResult.class);
      assertEquals(PaymentStatus.PAYMENT_CLOSED, result.getStatus());
      assertEquals(1, t.systems.paymentCreates.get());
      assertEquals("CLOSED", t.systems.payments.get("C1-v2-1").getStatus());
    }
  }

  @Test
  void aboveAuthorityNeedsADifferentPersonWithEnoughAuthority() throws Exception {
    try (StpTestEnv t = new StpTestEnv()) {
      PaymentWorkflow wf = start(t, request(2_500_000));
      OpenDecision task = t.systems.awaitTask("PAYMENT_APPROVAL");
      Decision.Builder d = Decision.newBuilder().setDecisionId(task.getDecisionId()).setChoice("APPROVE")
          .addActorRoles("payment-approver").setActorAuthorityCents(5_000_000);

      assertThrows(WorkflowUpdateException.class, () -> wf.decide(d.setActorId("adj-1").build()), "requester");
      assertThrows(WorkflowUpdateException.class,
          () -> wf.decide(d.setActorId("sup-9").setActorAuthorityCents(1_000_000).build()), "authority");
      assertEquals("APPROVE", wf.decide(d.setActorId("sup-2").setActorAuthorityCents(5_000_000).build()).getChoice());

      PaymentResult result = WorkflowStub.fromTyped(wf).getResult(PaymentResult.class);
      assertEquals(PaymentStatus.PAYMENT_CLOSED, result.getStatus());
      assertEquals("sup-2", result.getApprovedBy());
    }
  }

  @Test
  void cancelBeforeApprovalCancelsWithoutCreating() throws Exception {
    try (StpTestEnv t = new StpTestEnv()) {
      PaymentWorkflow wf = start(t, request(2_500_000));
      t.systems.awaitTask("PAYMENT_APPROVAL");
      wf.cancelPayment(CancelPayment.newBuilder().setRequestedBy("adj-1").build());
      PaymentResult result = WorkflowStub.fromTyped(wf).getResult(PaymentResult.class);
      assertEquals(PaymentStatus.PAYMENT_CANCELLED, result.getStatus());
      assertEquals(0, t.systems.paymentCreates.get());
      assertEquals(0, t.systems.openTasks.size(), "task withdrawn");
    }
  }
}
