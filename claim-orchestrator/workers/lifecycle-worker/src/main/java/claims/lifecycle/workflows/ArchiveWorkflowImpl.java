package claims.lifecycle.workflows;

import claims.contracts.v1.ArchiveReport;
import claims.contracts.v1.ArchiveRequest;
import claims.lifecycle.activities.ArchiveActivities;
import claims.runtime.Names;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class ArchiveWorkflowImpl implements ArchiveWorkflow {
  private final ArchiveActivities archive = Workflow.newActivityStub(ArchiveActivities.class,
      ActivityOptions.newBuilder()
          .setTaskQueue(Names.LIFECYCLE_QUEUE)
          .setStartToCloseTimeout(Duration.ofMinutes(15))
          .setHeartbeatTimeout(Duration.ofMinutes(1))
          .setRetryOptions(RetryOptions.newBuilder().setMaximumInterval(Duration.ofMinutes(5)).build())
          .build());

  @Override
  public ArchiveReport run(ArchiveRequest request) {
    ArchiveReport business = archive.archiveClosedClaims(request);
    ArchiveReport histories = archive.checkHistoryExports(request);
    return business.toBuilder()
        .setHistoriesChecked(histories.getHistoriesChecked())
        .setHistoriesWritten(histories.getHistoriesWritten())
        .build();
  }
}
