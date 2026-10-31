package claims.lifecycle.activities;

import claims.contracts.v1.ArchiveReport;
import claims.contracts.v1.ArchiveRequest;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ArchiveActivities {
  /** Copies business history of claims closed more than N days ago to S3, verifies it, then deletes it. */
  @ActivityMethod(name = "Archive.archiveClosedClaims")
  ArchiveReport archiveClosedClaims(ArchiveRequest request);

  /** Confirms every recently closed workflow has its history in S3; writes any missing one while retained. */
  @ActivityMethod(name = "Archive.checkHistoryExports")
  ArchiveReport checkHistoryExports(ArchiveRequest request);
}
