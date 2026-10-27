package com.example.claims.temporal.workflows.lifecycle;

import com.example.claims.temporal.Names;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.ClaimEventType;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackOutcome;
import com.example.claims.contracts.TrackRequest;
import com.example.claims.contracts.TrackResult;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Stand-ins registered under the real child workflow type names, so the parent is tested without
 * the Python TSA worker or the real STP. Behaviour is chosen from the claimId to stay deterministic.
 */
final class StubTracks {
  private StubTracks() {}

  static TrackResult result(TrackRequest r, TrackOutcome outcome, Track next, String reason) {
    return new TrackResult().withSchemaVersion(1).withClaimId(r.getClaimId()).withTrack(r.getTrack())
        .withStageVersion(r.getStageVersion()).withOutcome(outcome).withNextTrack(next).withReason(reason).withRuleId("stub");
  }

  @WorkflowInterface
  public interface Tsa {
    @WorkflowMethod(name = Names.TSA_WORKFLOW)
    TrackResult run(TrackRequest request);
  }

  public static class TsaImpl implements Tsa {
    @Override
    public TrackResult run(TrackRequest r) {
      Track next = r.getClaimId().startsWith("siu") ? Track.SIU : Track.STP;
      return result(r, TrackOutcome.ROUTE, next, "stub triage");
    }
  }

  @WorkflowInterface
  public interface Stp {
    @WorkflowMethod(name = Names.STP_WORKFLOW)
    TrackResult run(TrackRequest request);

    @SignalMethod(name = Names.SIGNAL_CLAIM_EVENT)
    void claimEvent(ClaimEvent event);
  }

  /**
   * claimId "retriage-N-..." : request re-triage while stageVersion < N, then complete.
   * claimId "wait-..."       : complete only after an ESTIMATE_RECEIVED event is forwarded by the parent.
   * otherwise               : complete immediately.
   */
  public static class StpImpl implements Stp {
    private boolean estimate;

    @Override
    public TrackResult run(TrackRequest r) {
      String id = r.getClaimId();
      if (id.startsWith("retriage-")) {
        int until = Integer.parseInt(id.split("-")[1]);
        if (r.getStageVersion() < until) {
          return result(r, TrackOutcome.RETRIAGE_REQUESTED, null, "stub change");
        }
      }
      if (id.startsWith("wait-")) {
        estimate = r.getBufferedEvents().stream().anyMatch(e -> e.getEventType() == ClaimEventType.ESTIMATE_RECEIVED);
        Workflow.await(() -> estimate);
      }
      return result(r, TrackOutcome.COMPLETED, null, "stub stp done");
    }

    @Override
    public void claimEvent(ClaimEvent event) {
      if (event.getEventType() == ClaimEventType.ESTIMATE_RECEIVED) {
        estimate = true;
      }
    }
  }
}
