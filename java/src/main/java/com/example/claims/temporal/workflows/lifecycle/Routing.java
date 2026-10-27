package com.example.claims.temporal.workflows.lifecycle;

import com.example.claims.contracts.AdjusterAction;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackResult;

/**
 * The whole lifecycle state machine as pure functions: easy to unit test, and the only place that
 * knows how tracks connect. Children report outcomes; they never pick the next track themselves
 * (TSA's ROUTE outcome is a recommendation the parent validates here).
 */
public final class Routing {
  private Routing() {}

  public record Next(Track track, String reason, boolean retriage) {
    static Next to(Track track, String reason) {
      return new Next(track, reason, false);
    }
  }

  public static Next initial(LifecycleSettings s, boolean reopened) {
    String why = reopened ? "claim reopened" : "FNOL received";
    return s.getTsaEnabled() ? Next.to(Track.TSA, why) : Next.to(Track.STP, why + " (pilot: TSA disabled)");
  }

  public static Next afterTrack(Track from, TrackResult r, LifecycleSettings s) {
    String reason = r.getReason() == null ? "" : r.getReason();
    switch (from) {
      case TSA:
        if (r.getOutcome() != com.example.claims.contracts.TrackOutcome.ROUTE) {
          return Next.to(Track.MANUAL, "TSA returned " + r.getOutcome() + ": " + reason);
        }
        Track target = r.getNextTrack();
        if (target == null || target == Track.UNKNOWN || target == Track.TSA || target == Track.CLOSED) {
          return Next.to(Track.MANUAL, "TSA route not allowed: " + target);
        }
        if (target == Track.IS && !s.getIsEnabled()) {
          return Next.to(Track.MANUAL, "IS disabled; " + reason);
        }
        return Next.to(target, "rule " + r.getRuleId() + ": " + reason);
      case STP:
      case IS:
        switch (r.getOutcome()) {
          case COMPLETED:
            return from == Track.STP
                ? Next.to(Track.CLOSED, "STP completed")
                : Next.to(retriageTarget(s), "investigation complete: " + reason);
          case RETRIAGE_REQUESTED:
            return new Next(retriageTarget(s), "re-triage requested: " + reason, true);
          case ADJUSTER_HANDOFF:
            return Next.to(Track.MANUAL, "handoff: " + reason);
          default:
            // UNKNOWN lands here when a newer child returns an outcome this parent build does not know.
            return Next.to(Track.MANUAL, "unrecognised outcome " + r.getOutcome());
        }
      default:
        throw new IllegalArgumentException("not a child track: " + from);
    }
  }

  public static Next afterDecision(AdjusterAction action, LifecycleSettings s) {
    switch (action) {
      case CLOSE:
        return Next.to(Track.CLOSED, "adjuster closed claim");
      case RETURN_TO_STP:
        return Next.to(Track.STP, "adjuster returned claim to STP");
      case RETRIAGE:
        return new Next(retriageTarget(s), "adjuster requested re-triage", true);
      default:
        throw new IllegalArgumentException("unsupported adjuster action " + action);
    }
  }

  private static Track retriageTarget(LifecycleSettings s) {
    return s.getTsaEnabled() ? Track.TSA : Track.MANUAL;
  }
}
