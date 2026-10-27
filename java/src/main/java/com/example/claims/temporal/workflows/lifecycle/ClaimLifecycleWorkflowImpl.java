package com.example.claims.temporal.workflows.lifecycle;

import com.example.claims.temporal.Names;
import com.example.claims.temporal.SearchAttrs;
import com.example.claims.temporal.activities.LifecycleActivities;
import com.example.claims.temporal.activities.ProjectionActivities;
import com.example.claims.contracts.AdjusterDecision;
import com.example.claims.contracts.AdjusterDecisionAck;
import com.example.claims.contracts.ClaimEvent;
import com.example.claims.contracts.ClaimEventType;
import com.example.claims.contracts.LifecycleCheckpoint;
import com.example.claims.contracts.LifecycleInput;
import com.example.claims.contracts.LifecycleSettings;
import com.example.claims.contracts.LifecycleState;
import com.example.claims.contracts.ProjectionRecord;
import com.example.claims.contracts.Track;
import com.example.claims.contracts.TrackRequest;
import com.example.claims.contracts.TrackResult;
import com.example.claims.contracts.Transition;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.common.SearchAttributes;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;

public class ClaimLifecycleWorkflowImpl implements ClaimLifecycleWorkflow {
  private static final Logger log = Workflow.getLogger(ClaimLifecycleWorkflowImpl.class);

  private static final Set<Track> CHILD_TRACKS = EnumSet.of(Track.TSA, Track.STP, Track.IS);
  private static final Set<Track> HUMAN_TRACKS = EnumSet.of(Track.MANUAL, Track.ESCALATION, Track.SIU);
  /** Which event types each child track wants forwarded. Everything else is buffered for the next track. */
  private static final Map<Track, Set<ClaimEventType>> RELEVANT = Map.of(
      Track.TSA, EnumSet.of(ClaimEventType.CLAIM_UPDATED),
      Track.STP, EnumSet.of(ClaimEventType.CLAIM_UPDATED, ClaimEventType.ESTIMATE_RECEIVED,
          ClaimEventType.PAYMENT_CANCEL_REQUESTED),
      Track.IS, EnumSet.of(ClaimEventType.CLAIM_UPDATED, ClaimEventType.DOCUMENT_RECEIVED));

  // Bounds keep the Continue-As-New checkpoint small no matter how many events a claim receives.
  private static final int MAX_SEEN_IDS = 500;
  private static final int MAX_TRANSITIONS_KEPT = 50;

  private final LifecycleActivities lifecycle = Workflow.newActivityStub(LifecycleActivities.class,
      ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofSeconds(30))
          .setRetryOptions(RetryOptions.newBuilder().setMaximumInterval(Duration.ofMinutes(1)).build())
          .build());
  private final ProjectionActivities projection = Workflow.newActivityStub(ProjectionActivities.class,
      ActivityOptions.newBuilder()
          .setTaskQueue(Names.PROJECTION_QUEUE)
          .setStartToCloseTimeout(Duration.ofSeconds(30))
          .setRetryOptions(RetryOptions.newBuilder().setMaximumInterval(Duration.ofMinutes(1)).build())
          .build());

  private String claimId;
  private LifecycleSettings settings;
  private LifecycleCheckpoint cp;
  private final Deque<ClaimEvent> inbox = new ArrayDeque<>();
  private final LinkedHashSet<String> seenEventIds = new LinkedHashSet<>();
  private final LinkedHashSet<String> seenDecisionIds = new LinkedHashSet<>();
  /** Coalesced by event type: a newer CLAIM_UPDATED replaces an older one. */
  private final LinkedHashMap<ClaimEventType, ClaimEvent> buffered = new LinkedHashMap<>();

  private ChildWorkflowStub activeChild;
  private String activeChildId;
  private Track activeChildTrack;
  private AdjusterDecision pendingDecision;
  private final List<Promise<Void>> projectionWrites = new ArrayList<>();
  private boolean reopenRequested;

  @Override
  public LifecycleState run(LifecycleInput input) {
    claimId = input.getClaimId();
    settings = input.getSettings() != null ? input.getSettings() : new LifecycleSettings();
    boolean resumed = input.getCheckpoint() != null;
    cp = resumed ? input.getCheckpoint() : new LifecycleCheckpoint();
    seenEventIds.addAll(cp.getSeenEventIds());
    seenDecisionIds.addAll(cp.getSeenDecisionIds());
    cp.getBufferedEvents().forEach(e -> buffered.put(e.getEventType(), e));

    Workflow.upsertTypedSearchAttributes(SearchAttrs.CLAIM_ID.valueSet(claimId));
    if (!resumed) {
      Routing.Next first = Routing.initial(settings, Boolean.TRUE.equals(input.getReopened()));
      cp.setCurrentTrack(Track.UNKNOWN);
      // Continue from ClaimsPro's stage so a reopened claim's new run neither fails ClaimsPro's
      // fencing (stage 1 < stage N) nor reuses the previous run's idempotency keys (which would
      // "replay" the old payment). Versioned: runs started before this change replay unchanged.
      int seed = 0;
      if (Workflow.getVersion("seed-stage-version", Workflow.DEFAULT_VERSION, 1) == 1) {
        seed = lifecycle.currentStageVersion(claimId);
      }
      cp.setStageVersion(seed);
      cp.setRetriageCount(0);
      transition(first);
    } else {
      log.info("Resumed after Continue-As-New at track={} stageVersion={}", cp.getCurrentTrack(), cp.getStageVersion());
      upsertTrackAttributes();
    }

    while (true) {
      Track track = cp.getCurrentTrack();
      Routing.Next next;
      if (CHILD_TRACKS.contains(track)) {
        next = runChildTrack(track);
      } else if (HUMAN_TRACKS.contains(track)) {
        next = awaitAdjuster(track);
        if (next == null) {
          // Long human wait grew the history: compact it, then keep waiting in the new run.
          continueAsNewIfDue();
          continue; // not taken because a decision just arrived: awaitAdjuster applies it
        }
      } else if (track == Track.CLOSED) {
        next = awaitReopenWindow();
        if (next == null) {
          // Reopen window elapsed: complete. A later reopen starts a new run with the same Workflow ID.
          record("LIFECYCLE_COMPLETED", "COMPLETED", "reopen window elapsed");
          Workflow.await(Workflow::isEveryHandlerFinished);
          flushProjectionWrites();
          return getState();
        }
      } else {
        next = Routing.Next.to(Track.MANUAL, "unexpected track " + track);
      }
      transition(next);

      continueAsNewIfDue();
    }
  }

  // ---------------------------------------------------------------- child tracks

  private Routing.Next runChildTrack(Track track) {
    int stageVersion = cp.getStageVersion();
    String childId = Names.childWorkflowId(claimId, track, stageVersion);
    ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
        .setWorkflowId(childId)
        .setTaskQueue(Names.taskQueueFor(track))
        .setTypedSearchAttributes(SearchAttributes.newBuilder()
            .set(SearchAttrs.CLAIM_ID, claimId)
            .set(SearchAttrs.CLAIM_TRACK, track.value())
            .set(SearchAttrs.STAGE_VERSION, (long) stageVersion)
            .build())
        .build();
    // Untyped stub: the parent depends on the child's name + contract, not on its code or language.
    ChildWorkflowStub child = Workflow.newUntypedChildWorkflowStub(Names.workflowTypeFor(track), options);
    TrackRequest request = new TrackRequest()
        .withSchemaVersion(1)
        .withClaimId(claimId)
        .withTrack(track)
        .withStageVersion(stageVersion)
        .withReason(lastTransitionReason())
        .withSettings(settings)
        .withBufferedEvents(new ArrayList<>(buffered.values()));
    buffered.clear();

    Promise<TrackResult> result = child.executeAsync(TrackResult.class, request);
    child.getExecution().get(); // wait for the child to start so events can be forwarded to it
    activeChild = child;
    activeChildId = childId;
    activeChildTrack = track;
    log.info("Started child {} on {}", childId, Names.taskQueueFor(track));

    while (true) {
      Workflow.await(() -> result.isCompleted() || !inbox.isEmpty());
      if (result.isCompleted()) {
        activeChild = null; // events still in the inbox are buffered for the next track
      }
      drainInbox();
      if (result.isCompleted()) {
        break;
      }
    }
    activeChildId = null;
    activeChildTrack = null;

    TrackResult r;
    try {
      r = result.get();
    } catch (ChildWorkflowFailure e) {
      log.warn("Child {} failed: {}", childId, e.getMessage());
      record("CHILD_FAILED", "FAILED", e.getMessage());
      return Routing.Next.to(Track.MANUAL, track + " failed: " + rootMessage(e));
    }
    if (r.getStageVersion() == null || r.getStageVersion() != stageVersion) {
      // Defensive lease check: never act on a result issued for a different stage.
      log.warn("Ignoring stale result stageVersion={} expected={}", r.getStageVersion(), stageVersion);
      return Routing.Next.to(Track.MANUAL, "stale child result");
    }
    return Routing.afterTrack(track, r, settings);
  }

  // ---------------------------------------------------------------- human tracks

  private Routing.Next awaitAdjuster(Track track) {
    // pendingDecision is NOT reset here: transition() clears it before the track changes, and an
    // Update can be accepted while transition()'s activities are still running (the validator
    // already sees the new track). Resetting here silently dropped that accepted decision.
    // Versioned so executions already waiting under the old code replay unchanged.
    if (Workflow.getVersion("keep-early-adjuster-decision", Workflow.DEFAULT_VERSION, 1) == Workflow.DEFAULT_VERSION) {
      pendingDecision = null;
    }
    Duration sla = Duration.ofSeconds(settings.getManualSlaSeconds());
    long deadline = Workflow.currentTimeMillis() + sla.toMillis();
    while (pendingDecision == null) {
      long remaining = deadline - Workflow.currentTimeMillis();
      if (remaining <= 0) {
        if (track == Track.MANUAL) {
          return Routing.Next.to(Track.ESCALATION, "manual SLA of " + sla + " breached");
        }
        record("SLA_BREACHED", "WARN", track + " still waiting for a decision");
        if (shouldContinueAsNew() && Workflow.getVersion("can-while-waiting", Workflow.DEFAULT_VERSION, 1) == 1) {
          return null;
        }
        deadline = Workflow.currentTimeMillis() + sla.toMillis();
        continue;
      }
      Workflow.await(Duration.ofMillis(remaining), () -> pendingDecision != null || !inbox.isEmpty());
      drainInbox();
    }
    AdjusterDecision d = pendingDecision;
    pendingDecision = null;
    return Routing.afterDecision(d.getAction(), settings);
  }

  @Override
  public void validateAdjusterDecision(AdjusterDecision d) {
    if (d.getDecisionId() == null || !claimId.equals(d.getClaimId())) {
      throw new IllegalArgumentException("decision must carry decisionId and this claimId");
    }
    if (seenDecisionIds.contains(d.getDecisionId())) {
      return; // idempotent retry of an accepted decision: accept again, handler returns duplicate=true
    }
    if (!HUMAN_TRACKS.contains(cp.getCurrentTrack()) || pendingDecision != null) {
      throw new IllegalStateException("claim is in " + cp.getCurrentTrack() + "; no adjuster decision expected");
    }
    if (d.getAction() == null || d.getAction() == com.example.claims.contracts.AdjusterAction.UNKNOWN) {
      throw new IllegalArgumentException("unknown action");
    }
  }

  @Override
  public AdjusterDecisionAck submitAdjusterDecision(AdjusterDecision d) {
    AdjusterDecisionAck ack = new AdjusterDecisionAck()
        .withSchemaVersion(1)
        .withDecisionId(d.getDecisionId())
        .withTrackAtDecision(cp.getCurrentTrack());
    if (!seenDecisionIds.add(d.getDecisionId())) {
      return ack.withDuplicate(true);
    }
    trim(seenDecisionIds);
    pendingDecision = d;
    log.info("Adjuster {} decided {} on {}", d.getAdjusterId(), d.getAction(), cp.getCurrentTrack());
    return ack.withDuplicate(false);
  }

  // ---------------------------------------------------------------- closed / reopen

  private Routing.Next awaitReopenWindow() {
    reopenRequested = false;
    long deadline = Workflow.currentTimeMillis() + settings.getReopenWindowSeconds() * 1000L;
    while (!reopenRequested) {
      long remaining = deadline - Workflow.currentTimeMillis();
      if (remaining <= 0) {
        return null;
      }
      Workflow.await(Duration.ofMillis(remaining), () -> reopenRequested || !inbox.isEmpty());
      drainInbox();
    }
    return Routing.initial(settings, true);
  }

  // ---------------------------------------------------------------- events

  @Override
  public void claimEvent(ClaimEvent event) {
    // Handlers only record; the main loop acts. That keeps ordering simple and replay-safe.
    if (event.getEventId() == null || !seenEventIds.add(event.getEventId())) {
      log.info("Duplicate event {} ignored", event.getEventId());
      return;
    }
    trim(seenEventIds);
    inbox.add(event);
  }

  private void drainInbox() {
    while (!inbox.isEmpty()) {
      ClaimEvent e = inbox.poll();
      ClaimEventType type = e.getEventType();
      if (type == ClaimEventType.FNOL_RECEIVED) {
        continue; // FNOL only starts the workflow (Signal-With-Start)
      }
      if (type == ClaimEventType.CLAIM_REOPENED) {
        if (cp.getCurrentTrack() == Track.CLOSED) {
          reopenRequested = true;
        } else {
          log.info("Reopen ignored: claim is open in {}", cp.getCurrentTrack());
        }
        continue;
      }
      if (activeChild != null && RELEVANT.getOrDefault(activeChildTrack, Set.of()).contains(type)) {
        try {
          activeChild.signal(Names.SIGNAL_CLAIM_EVENT, e);
          log.info("Forwarded {} {} to {}", type, e.getEventId(), activeChildId);
          continue;
        } catch (Exception ex) {
          log.info("Forward to {} failed ({}); buffering", activeChildId, ex.getMessage());
        }
      }
      buffered.put(type, e);
    }
  }

  // ---------------------------------------------------------------- bookkeeping

  private void transition(Routing.Next next) {
    Track from = cp.getCurrentTrack();
    Track to = next.track();
    if (next.retriage()) {
      cp.setRetriageCount(cp.getRetriageCount() + 1);
      if (cp.getRetriageCount() > settings.getMaxRetriage()) {
        to = Track.ESCALATION;
        next = new Routing.Next(to, "re-triage limit (" + settings.getMaxRetriage() + ") exceeded; " + next.reason(), false);
      }
    }
    // Every track entry gets a new stage version: the lease token children and ClaimsPro writes carry.
    int stageVersion = cp.getStageVersion() + 1;
    pendingDecision = null; // a decision belongs to the stage it was accepted in
    cp.setStageVersion(stageVersion);
    cp.setCurrentTrack(to);
    cp.getTransitions().add(new Transition()
        .withFrom(from).withTo(to).withStageVersion(stageVersion).withReason(next.reason()).withAt(now()));
    while (cp.getTransitions().size() > MAX_TRANSITIONS_KEPT) {
      cp.getTransitions().remove(0);
    }
    log.info("Transition {} -> {} (stageVersion={}): {}", from, to, stageVersion, next.reason());
    lifecycle.beginStage(claimId, stageVersion, to);
    upsertTrackAttributes();
    record("TRANSITION:" + from + "->" + to, "OK", next.reason());
  }

  private void record(String step, String status, String detail) {
    ProjectionRecord row = new ProjectionRecord()
        .withSchemaVersion(1)
        .withClaimId(claimId)
        .withStageVersion(cp.getStageVersion())
        .withTrack(cp.getCurrentTrack())
        .withStep(step)
        .withStatus(status)
        .withDetail(detail)
        .withWorkflowId(Workflow.getInfo().getWorkflowId())
        .withRunId(Workflow.getInfo().getRunId())
        .withOccurredAt(now());
    if (Workflow.getVersion("async-parent-projection", Workflow.DEFAULT_VERSION, 1) == 1) {
      // Do not block the lifecycle on the history store; flushed before completion / Continue-As-New.
      projectionWrites.add(Async.procedure(projection::recordProjection, row));
    } else {
      projection.recordProjection(row);
    }
  }

  private void flushProjectionWrites() {
    Promise.allOf(projectionWrites).get();
    projectionWrites.clear();
  }

  private void upsertTrackAttributes() {
    Workflow.upsertTypedSearchAttributes(
        SearchAttrs.CLAIM_TRACK.valueSet(cp.getCurrentTrack().value()),
        SearchAttrs.STAGE_VERSION.valueSet((long) cp.getStageVersion()),
        SearchAttrs.RETRIAGE_COUNT.valueSet((long) cp.getRetriageCount()));
  }

  /** Continues as new when history is large. Never returns in that case. */
  private void continueAsNewIfDue() {
    if (!shouldContinueAsNew()) {
      return;
    }
    Workflow.await(Workflow::isEveryHandlerFinished);
    if (pendingDecision != null
        && Workflow.getVersion("keep-pending-decision-over-can", Workflow.DEFAULT_VERSION, 1) == 1) {
      // An accepted adjuster decision is not part of the checkpoint: apply it first, compact later.
      return;
    }
    flushProjectionWrites();
    log.info("Continue-As-New at historyLength={}", Workflow.getInfo().getHistoryLength());
    Workflow.continueAsNew(new LifecycleInput()
        .withSchemaVersion(1)
        .withClaimId(claimId)
        .withSettings(settings)
        .withCheckpoint(checkpoint()));
  }

  private boolean shouldContinueAsNew() {
    return Workflow.getInfo().isContinueAsNewSuggested()
        || Workflow.getInfo().getHistoryLength() > settings.getContinueAsNewEventThreshold();
  }

  private LifecycleCheckpoint checkpoint() {
    return cp
        .withSeenEventIds(new ArrayList<>(seenEventIds))
        .withSeenDecisionIds(new ArrayList<>(seenDecisionIds))
        .withBufferedEvents(new ArrayList<>(buffered.values()));
  }

  @Override
  public LifecycleState getState() {
    return new LifecycleState()
        .withSchemaVersion(1)
        .withClaimId(claimId)
        .withCurrentTrack(cp == null ? Track.UNKNOWN : cp.getCurrentTrack())
        .withStageVersion(cp == null ? 0 : cp.getStageVersion())
        .withRetriageCount(cp == null ? 0 : cp.getRetriageCount())
        .withActiveChildWorkflowId(activeChildId)
        .withBufferedEventCount(buffered.size())
        .withTransitions(cp == null ? List.of() : new ArrayList<>(cp.getTransitions()));
  }

  private String lastTransitionReason() {
    List<Transition> t = cp.getTransitions();
    return t.isEmpty() ? "" : t.get(t.size() - 1).getReason();
  }

  private static String now() {
    return Instant.ofEpochMilli(Workflow.currentTimeMillis()).toString();
  }

  private static void trim(LinkedHashSet<String> ids) {
    while (ids.size() > MAX_SEEN_IDS) {
      ids.remove(ids.iterator().next());
    }
  }

  private static String rootMessage(Throwable t) {
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.getMessage();
  }
}
