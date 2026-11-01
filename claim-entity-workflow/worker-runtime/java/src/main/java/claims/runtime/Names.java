package claims.runtime;

/** Task queues, deployments, workflow types, message names and workflow IDs used by every worker. */
public final class Names {
  private Names() {}

  // Task queues
  public static final String LIFECYCLE_QUEUE = "claims-lifecycle";
  public static final String TSA_QUEUE = "claims-tsa";
  public static final String IS_QUEUE = "claims-is";
  public static final String STP_QUEUE = "claims-stp";
  public static final String CLAIMSPRO_QUEUE = "claimspro-activities";

  // Worker deployments (Worker Versioning)
  public static final String LIFECYCLE_DEPLOYMENT = "claims-lifecycle";
  public static final String STP_DEPLOYMENT = "claims-stp";
  public static final String CLAIMSPRO_DEPLOYMENT = "claims-claimspro";

  // Workflow types: one workflow per claim, plus the nightly archive
  public static final String CLAIM = "ClaimWorkflow";
  public static final String ARCHIVE = "ArchiveWorkflow";

  // Messages
  public static final String SIGNAL_CLAIM_EVENT = "claimEvent";
  public static final String SIGNAL_CANCEL_PAYMENT = "cancelPayment";
  public static final String UPDATE_DECIDE = "decide";

  // Schedules
  public static final String ARCHIVE_SCHEDULE = "claims-nightly-archive";

  public static String claimWorkflowId(String claimId) {
    return "claim:" + claimId;
  }


  public static String paymentKey(String claimId, int stageVersion, int n) {
    return claimId + "-v" + stageVersion + "-" + n;
  }

}
