package claims.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import claims.contracts.ClaimEvent;
import claims.contracts.ClaimEventType;
import claims.contracts.Decision;
import claims.contracts.OpenDecision;
import claims.contracts.Stage;
import claims.lifecycle.workflows.ClaimWorkflow;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import org.junit.jupiter.api.Test;

class ClaimWorkflowTest {
  private static boolean hasRow(ClaimTestEnv t, String step) {
    return t.systems.rows.stream().anyMatch(r -> r.getStep().equals(step));
  }

  @Test
  void straightThroughPaysOnceAndCloses() {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      WorkflowStub.fromTyped(t.start("C1", 400_000)).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C1").getStage());
      assertEquals(1, t.systems.paymentCreates.get());
      assertEquals(1, t.systems.closedClaims.size());
      assertTrue(t.systems.rows.get(t.systems.rows.size() - 1).getClosing());
    }
  }

  @Test
  void injuryDuringTheEstimateStageCancelsItAndReTriages() throws Exception {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      t.tracks.estimateAvailable = false;
      ClaimWorkflow wf = t.start("C2", 400_000);
      t.awaitRow("stp.stage:await_estimate");
      t.systems.claims.computeIfPresent("C2", (k, c) -> c.toBuilder().setInjuryReported(true).setChangeVersion(2).build());
      wf.claimEvent(ClaimEvent.newBuilder().setEventId("u1").setClaimId("C2").setType(ClaimEventType.CLAIM_UPDATED).build());

      OpenDecision task = t.systems.awaitTask("MANUAL_HANDLING");
      assertTrue(hasRow(t, "stp.stage.cancelled:await_estimate"), "the estimate stage was cancelled at once");
      t.tracks.estimateAvailable = true;
      wf.decide(Decision.newBuilder().setDecisionId(task.getDecisionId()).setChoice("RETURN_TO_STP")
          .setActorId("adj-7").addActorRoles("adjuster").build());

      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C2").getStage());
      assertEquals(1, t.systems.paymentCreates.get());
    }
  }

  @Test
  void paymentAboveAuthorityNeedsADifferentPersonWithAReasonToReject() throws Exception {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      t.tracks.estimateCents = 2_500_000;
      ClaimWorkflow wf = t.start("C3", 2_600_000);
      OpenDecision task = t.systems.awaitTask("PAYMENT_APPROVAL");
      Decision.Builder d = Decision.newBuilder().setDecisionId(task.getDecisionId())
          .addActorRoles("payment-approver").setActorAuthorityCents(5_000_000);

      assertThrows(WorkflowUpdateException.class, () -> wf.decide(d.setChoice("APPROVE").setActorId("adj-1").build()), "requester");
      assertThrows(WorkflowUpdateException.class, () -> wf.decide(d.setChoice("REJECT").setActorId("sup-2").build()), "no reason");
      assertEquals("APPROVE", wf.decide(d.setChoice("APPROVE").setActorId("sup-2").build()).getChoice());

      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(1, t.systems.paymentCreates.get());
      assertTrue(hasRow(t, "payment.approved:C3-v2-1"));
    }
  }

  @Test
  void claimClosedInClaimsProCancelsTheRunningModule() throws Exception {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      t.tracks.estimateAvailable = false;
      ClaimWorkflow wf = t.start("C4", 400_000);
      t.awaitRow("stp.stage:await_estimate");
      wf.claimEvent(ClaimEvent.newBuilder().setEventId("x1").setClaimId("C4").setType(ClaimEventType.CLAIM_CLOSED).build());
      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C4").getStage());
      assertTrue(hasRow(t, "module.cancelled:stp"));
      assertTrue(t.systems.closedClaims.isEmpty(), "already closed in ClaimsPro");
    }
  }

  @Test
  void pilotScopeGoesStraightToStp() {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      t.systems.policy = claims.runtime.testing.FakeSystems.defaultPolicy().toBuilder().setPilotStpOnly(true).build();
      WorkflowStub.fromTyped(t.start("C5", 400_000)).getResult(Void.class);
      assertTrue(t.systems.rows.stream().noneMatch(r -> r.getStep().equals("triage.ruling")));
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C5").getStage());
    }
  }

  @Test
  void paymentCancelledInClaimsProDuringApprovalGoesToAPerson() throws Exception {
    try (ClaimTestEnv t = new ClaimTestEnv()) {
      t.tracks.estimateCents = 1_800_000;
      ClaimWorkflow wf = t.start("C6", 1_900_000);
      t.systems.awaitTask("PAYMENT_APPROVAL");
      wf.claimEvent(ClaimEvent.newBuilder().setEventId("pc1").setClaimId("C6").setType(ClaimEventType.PAYMENT_CANCEL_REQUESTED).build());
      OpenDecision task = t.systems.awaitTask("MANUAL_HANDLING");
      wf.decide(Decision.newBuilder().setDecisionId(task.getDecisionId()).setChoice("CLOSE").setActorId("adj-7")
          .addActorRoles("adjuster").setReason("customer withdrew").build());
      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(0, t.systems.paymentCreates.get());
      assertTrue(hasRow(t, "payment.result:C6-v2-1"));
    }
  }
}
