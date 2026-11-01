package claims.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import claims.contracts.Stage;
import claims.contracts.TrackOutcome;
import claims.contracts.TrackResult;
import claims.lifecycle.workflows.Routing;
import org.junit.jupiter.api.Test;

/** The routing table is the parent's whole behaviour: every route is pinned here. */
class RoutingTest {
  private static Routing.Next after(Stage from, TrackOutcome o) {
    return Routing.afterTrack(from, TrackResult.newBuilder().setOutcome(o).setReason("r").build());
  }

  @Test
  void tracksRouteOnlyWhereTheTableAllows() {
    assertEquals(Stage.STAGE_STP, after(Stage.STAGE_TRIAGE, TrackOutcome.ROUTE_STP).stage());
    assertEquals(Stage.STAGE_SIU, after(Stage.STAGE_TRIAGE, TrackOutcome.ROUTE_SIU).stage());
    assertEquals(Stage.STAGE_MANUAL, after(Stage.STAGE_TRIAGE, TrackOutcome.COMPLETED).stage(), "triage cannot close");
    assertEquals(Stage.STAGE_CLOSED, after(Stage.STAGE_STP, TrackOutcome.COMPLETED).stage());
    assertTrue(after(Stage.STAGE_STP, TrackOutcome.RETRIAGE).retriage());
    assertTrue(after(Stage.STAGE_INVESTIGATION, TrackOutcome.RETRIAGE).retriage());
    assertEquals(Stage.STAGE_MANUAL, after(Stage.STAGE_STP, TrackOutcome.TRACK_OUTCOME_UNSPECIFIED).stage(), "unknown goes to a person");
  }

  @Test
  void humanDecisionsRoute() {
    assertTrue(Routing.afterDecision(Stage.STAGE_MANUAL, "CLOSE").closeInClaimsPro());
    assertEquals(Stage.STAGE_STP, Routing.afterDecision(Stage.STAGE_MANUAL, "RETURN_TO_STP").stage());
    assertEquals(Stage.STAGE_ESCALATION, Routing.afterDecision(Stage.STAGE_MANUAL, "DEADLINE_PASSED").stage());
    assertEquals(Stage.STAGE_TRIAGE, Routing.afterDecision(Stage.STAGE_SIU, "CLEAR").stage());
    assertTrue(Routing.afterDecision(Stage.STAGE_SIU, "DENY").closeInClaimsPro());
    assertEquals(Stage.STAGE_TRIAGE, Routing.afterDecision(Stage.STAGE_REOPEN_REVIEW, "APPROVE").stage());
  }
}
