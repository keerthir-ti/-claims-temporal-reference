package claims.lifecycle.archive;

/**
 * The S3 archive. Local runs use {@link LocalArchiveStore}; production uses an S3 implementation
 * (bucket with versioning, object lock and the client's KMS key). Keys:
 * business/close_date=YYYY-MM-DD/{claimId}.jsonl and the history export location.
 */
public interface ArchiveStore {
  void put(String key, byte[] data);

  byte[] get(String key);

  boolean exists(String key);

  /** Where Temporal Cloud's history export (or this archive's fallback copy) keeps a run's history. */
  String historyKey(String workflowId, String runId);
}
