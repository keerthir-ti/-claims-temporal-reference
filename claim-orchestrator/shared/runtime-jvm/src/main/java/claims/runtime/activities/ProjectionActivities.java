package claims.runtime.activities;

import claims.contracts.v1.ClaimRef;
import claims.contracts.v1.Done;
import claims.contracts.v1.ProjectionEvent;
import claims.contracts.v1.RecordCheck;
import claims.contracts.v1.StageChange;
import claims.contracts.v1.StageInfo;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Business history in the projection table. Runs on the Lifecycle worker; called from every workflow. */
@ActivityInterface
public interface ProjectionActivities {
  /** Insert-if-absent; a repeat of the same row does nothing. */
  @ActivityMethod(name = "Projection.record")
  Done record(ProjectionEvent event);

  /** Records the claim's new stage version; writes from older stage versions are refused from now on. */
  @ActivityMethod(name = "Projection.beginStage")
  Done beginStage(StageChange change);

  @ActivityMethod(name = "Projection.currentStage")
  StageInfo currentStage(ClaimRef ref);

  /** Fails (retryable) while any run of the claim has missing rows or no closing row. */
  @ActivityMethod(name = "Projection.verifyRecord")
  Done verifyRecord(RecordCheck check);
}
