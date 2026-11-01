package claims.runtime.workflow;

import claims.contracts.DecisionPolicy;
import claims.runtime.Names;
import claims.runtime.activities.ActivityErrors;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.activities.PolicyActivities;
import claims.runtime.activities.ProjectionActivities;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** Activity stubs with the timeouts and retries from the design. Workflow code only. */
public final class ActivityStubs {
  private ActivityStubs() {}

  /** Backoff 1 s to 60 s; business failures are not retried; retries stop when the retry window ends. */
  public static ActivityOptions options(String taskQueue, Duration perAttempt, Duration retryWindow) {
    ActivityOptions.Builder b = ActivityOptions.newBuilder()
        .setTaskQueue(taskQueue)
        .setStartToCloseTimeout(perAttempt)
        .setRetryOptions(RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(1))
            .setBackoffCoefficient(2.0)
            .setMaximumInterval(Duration.ofSeconds(60))
            .setDoNotRetry(ActivityErrors.BUSINESS.toArray(new String[0]))
            .build());
    if (retryWindow != null) {
      b.setScheduleToCloseTimeout(retryWindow);
    }
    return b.build();
  }

  public static Duration retryWindow(DecisionPolicy policy) {
    return Duration.ofSeconds(policy.getRetryWindowSeconds() > 0 ? policy.getRetryWindowSeconds() : 3600);
  }

  /** Projection writes retry without limit: the workflow does not move on until the row is written. */
  public static ProjectionActivities projection() {
    return Workflow.newActivityStub(ProjectionActivities.class,
        options(Names.LIFECYCLE_QUEUE, Duration.ofSeconds(10), null));
  }

  public static PolicyActivities policy() {
    return Workflow.newActivityStub(PolicyActivities.class,
        options(Names.LIFECYCLE_QUEUE, Duration.ofSeconds(10), null));
  }

  public static ClaimsProActivities claimsPro(Duration retryWindow) {
    return Workflow.newActivityStub(ClaimsProActivities.class,
        options(Names.CLAIMSPRO_QUEUE, Duration.ofSeconds(30), retryWindow));
  }
}
