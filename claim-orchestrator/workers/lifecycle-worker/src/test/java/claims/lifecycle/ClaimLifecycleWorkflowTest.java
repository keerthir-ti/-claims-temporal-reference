package claims.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import claims.contracts.v1.ClaimEvent;
import claims.contracts.v1.ClaimEventType;
import claims.contracts.v1.Decision;
import claims.contracts.v1.OpenDecision;
import claims.contracts.v1.Stage;
import claims.contracts.v1.TrackOutcome;
import claims.lifecycle.workflows.ClaimLifecycleWorkflow;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import org.junit.jupiter.api.Test;

class ClaimLifecycleWorkflowTest {
  @Test
  void triageThenStpClosesTheClaim() {
    try (LifecycleTestEnv t = new LifecycleTestEnv()) {
      LifecycleTestEnv.TRIAGE.add(TrackOutcome.ROUTE_STP);
      LifecycleTestEnv.STP.add(TrackOutcome.COMPLETED);
      WorkflowStub.fromTyped(t.start("C1")).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C1").getStage());
      assertEquals(3, t.systems.stages.get("C1").getStep().getStageVersion());
      assertTrue(t.systems.rows.get(t.systems.rows.size() - 1).getClosing());
    }
  }

  @Test
  void manualDecisionIsValidatedAndReturnsTheClaimToStp() throws Exception {
    try (LifecycleTestEnv t = new LifecycleTestEnv()) {
      LifecycleTestEnv.TRIAGE.add(TrackOutcome.ROUTE_MANUAL);
      LifecycleTestEnv.STP.add(TrackOutcome.COMPLETED);
      ClaimLifecycleWorkflow wf = t.start("C2");
      OpenDecision task = t.systems.awaitTask("MANUAL_HANDLING");
      Decision.Builder d = Decision.newBuilder().setDecisionId(task.getDecisionId()).setChoice("RETURN_TO_STP").setActorId("adj-7");

      assertThrows(WorkflowUpdateException.class, () -> wf.decide(d.addActorRoles("viewer").build()), "role");
      assertEquals("RETURN_TO_STP", wf.decide(d.clearActorRoles().addActorRoles("adjuster").build()).getChoice());
      assertEquals("RETURN_TO_STP", wf.decide(d.build()).getChoice(), "a repeated decision returns the first result");

      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C2").getStage());
    }
  }

  @Test
  void claimClosedInClaimsProCancelsTheActiveTrack() throws Exception {
    try (LifecycleTestEnv t = new LifecycleTestEnv()) {
      LifecycleTestEnv.TRIAGE.add(TrackOutcome.ROUTE_STP);
      LifecycleTestEnv.STP.add(LifecycleTestEnv.HANG);
      ClaimLifecycleWorkflow wf = t.start("C3");
      while (t.systems.rows.stream().noneMatch(r -> r.getStep().equals("track.start:stp"))) {
        Thread.sleep(25);
      }
      wf.claimEvent(ClaimEvent.newBuilder().setEventId("closed-1").setClaimId("C3").setType(ClaimEventType.CLAIM_CLOSED).build());
      WorkflowStub.fromTyped(wf).getResult(Void.class);
      assertEquals(Stage.STAGE_CLOSED, t.systems.stages.get("C3").getStage());
      assertTrue(t.systems.rows.stream().anyMatch(r -> r.getStep().equals("track.cancelled:stp")));
      assertTrue(t.systems.closedClaims.isEmpty(), "already closed in ClaimsPro: not closed again");
    }
  }
}
