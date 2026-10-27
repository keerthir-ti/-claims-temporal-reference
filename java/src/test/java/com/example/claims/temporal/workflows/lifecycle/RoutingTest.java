package com.example.claims.temporal.workflows.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.claims.contracts.AdjusterAction;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackOutcome;
import com.example.claims.contracts.TrackResult;
import org.junit.jupiter.api.Test;

class RoutingTest {
  private final LifecycleSettings all = new LifecycleSettings();
  private final LifecycleSettings pilot = new LifecycleSettings().withTsaEnabled(false).withIsEnabled(false);

  private static TrackResult result(TrackOutcome outcome, Track next) {
    return new TrackResult().withOutcome(outcome).withNextTrack(next).withRuleId("r");
  }

  @Test
  void initialTrackDependsOnPilotScope() {
    assertEquals(Track.TSA, Routing.initial(all, false).track());
    assertEquals(Track.STP, Routing.initial(pilot, false).track());
  }

  @Test
  void tsaRouteIsValidatedByParent() {
    assertEquals(Track.STP, Routing.afterTrack(Track.TSA, result(TrackOutcome.ROUTE, Track.STP), all).track());
    assertEquals(Track.MANUAL, Routing.afterTrack(Track.TSA, result(TrackOutcome.ROUTE, Track.CLOSED), all).track());
    assertEquals(Track.MANUAL, Routing.afterTrack(Track.TSA, result(TrackOutcome.ROUTE, Track.UNKNOWN), all).track());
    assertEquals(Track.MANUAL, Routing.afterTrack(Track.TSA, result(TrackOutcome.ROUTE, Track.IS), pilot).track());
  }

  @Test
  void stpOutcomes() {
    assertEquals(Track.CLOSED, Routing.afterTrack(Track.STP, result(TrackOutcome.COMPLETED, null), all).track());
    Routing.Next retriage = Routing.afterTrack(Track.STP, result(TrackOutcome.RETRIAGE_REQUESTED, null), all);
    assertEquals(Track.TSA, retriage.track());
    assertTrue(retriage.retriage());
    assertEquals(Track.MANUAL, Routing.afterTrack(Track.STP, result(TrackOutcome.RETRIAGE_REQUESTED, null), pilot).track());
    // A newer child returning an outcome this build does not know decodes as UNKNOWN -> safe default.
    assertEquals(Track.MANUAL, Routing.afterTrack(Track.STP, result(TrackOutcome.UNKNOWN, null), all).track());
  }

  @Test
  void investigationReturnsToTriage() {
    assertEquals(Track.TSA, Routing.afterTrack(Track.IS, result(TrackOutcome.COMPLETED, null), all).track());
  }

  @Test
  void adjusterDecisions() {
    assertEquals(Track.CLOSED, Routing.afterDecision(AdjusterAction.CLOSE, all).track());
    assertEquals(Track.STP, Routing.afterDecision(AdjusterAction.RETURN_TO_STP, all).track());
    assertTrue(Routing.afterDecision(AdjusterAction.RETRIAGE, all).retriage());
  }
}
