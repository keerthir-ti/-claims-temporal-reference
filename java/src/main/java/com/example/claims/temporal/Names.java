package com.example.claims.temporal;

import com.example.claims.temporal.workflows.lifecycle.ClaimLifecycleWorkflow;
import com.example.claims.temporal.workflows.stp.StpWorkflow;
import com.example.claims.temporal.workflows.stp.VendorDispatchWorkflow;
import com.example.claims.contracts.Track;

/**
 * Stable cross-language names. Workflow types, signal/update/query names and task queues are the
 * real contract between independently deployed workers, so they live in one place and never
 * change once a workflow has been started with them.
 */
public final class Names {
  private Names() {}

  public static final String LIFECYCLE_QUEUE = "claims-lifecycle";
  public static final String STP_QUEUE = "claims-stp";
  public static final String TSA_QUEUE = "claims-tsa";
  public static final String IS_QUEUE = "claims-is";
  /** Projection writes; hosted by the Java lifecycle worker, called from Java and Python. */
  public static final String PROJECTION_QUEUE = "claims-projection";

  /** Worker Deployment names (Worker Versioning). Python: claims-tsa, claims-is. */
  public static final String LIFECYCLE_DEPLOYMENT = "claims-lifecycle";
  public static final String STP_DEPLOYMENT = "claims-stp";

  public static final String LIFECYCLE_WORKFLOW = "ClaimLifecycleWorkflow";
  public static final String STP_WORKFLOW = "StpWorkflow";
  public static final String TSA_WORKFLOW = "TsaWorkflow";
  public static final String IS_WORKFLOW = "InvestigationWorkflow";
  public static final String VENDOR_DISPATCH_WORKFLOW = "VendorDispatchWorkflow";

  public static final String SIGNAL_CLAIM_EVENT = "claimEvent";
  public static final String UPDATE_ADJUSTER_DECISION = "submitAdjusterDecision";
  public static final String QUERY_STATE = "getState";

  public static final String ACTIVITY_RECORD_PROJECTION = "recordProjection";

  /** claim:{claimId}. ':' keeps the claimId recoverable even when it contains '-'. */
  public static String lifecycleWorkflowId(String claimId) {
    return "claim:" + claimId;
  }

  /** claim:{claimId}:{track}:{stageVersion}. One execution per track visit. */
  public static String childWorkflowId(String claimId, Track track, int stageVersion) {
    return lifecycleWorkflowId(claimId) + ":" + track.value().toLowerCase() + ":" + stageVersion;
  }

  public static String workflowTypeFor(Track track) {
    switch (track) {
      case STP: return STP_WORKFLOW;
      case TSA: return TSA_WORKFLOW;
      case IS: return IS_WORKFLOW;
      default: throw new IllegalArgumentException("Track has no child workflow: " + track);
    }
  }

  public static String taskQueueFor(Track track) {
    switch (track) {
      case STP: return STP_QUEUE;
      case TSA: return TSA_QUEUE;
      case IS: return IS_QUEUE;
      default: throw new IllegalArgumentException("Track has no task queue: " + track);
    }
  }
}
