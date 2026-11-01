package claims.lifecycle;

import claims.contracts.ArchiveRequest;
import claims.lifecycle.activities.ArchiveActivitiesImpl;
import claims.lifecycle.activities.PolicyActivitiesImpl;
import claims.lifecycle.activities.ProjectionActivitiesImpl;
import claims.lifecycle.archive.LocalArchiveStore;
import claims.lifecycle.workflows.ArchiveWorkflowImpl;
import claims.lifecycle.workflows.ClaimLifecycleWorkflowImpl;
import claims.runtime.Env;
import claims.runtime.Names;
import claims.runtime.db.Database;
import claims.runtime.temporal.TemporalConnection;
import claims.runtime.temporal.WorkerSettings;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.schedules.Schedule;
import io.temporal.client.schedules.ScheduleActionStartWorkflow;
import io.temporal.client.schedules.ScheduleAlreadyRunningException;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.client.schedules.ScheduleOptions;
import io.temporal.client.schedules.ScheduleSpec;
import io.temporal.common.VersioningBehavior;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.nio.file.Path;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lifecycle worker (claims-lifecycle). ClaimLifecycleWorkflow lives for the whole claim, so it runs on
 * Auto-Upgrade and changes are made with patching. Also hosts the projection, policy and archive
 * activities and the nightly archive schedule.
 */
public final class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  private Main() {}

  public static void main(String[] args) {
    WorkflowServiceStubs service = TemporalConnection.service();
    WorkflowClient client = TemporalConnection.client(service);
    DataSource projection = Database.pool("PROJECTION", Env.getInt("DB_POOL_SIZE", 10));

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(Names.LIFECYCLE_QUEUE,
        WorkerSettings.options(Names.LIFECYCLE_DEPLOYMENT, VersioningBehavior.AUTO_UPGRADE).build());
    worker.registerWorkflowImplementationTypes(ClaimLifecycleWorkflowImpl.class, ArchiveWorkflowImpl.class);
    worker.registerActivitiesImplementations(
        new ProjectionActivitiesImpl(projection),
        new PolicyActivitiesImpl(Path.of(Env.get("POLICY_FILE", "config/decision-policy.yaml"))),
        // Replace LocalArchiveStore with the S3 implementation.
        new ArchiveActivitiesImpl(projection, new LocalArchiveStore(Path.of(Env.get("ARCHIVE_DIR", "/data/archive"))), client));
    factory.start();
    ensureArchiveSchedule(service);
    log.info("Lifecycle worker polling {}", Names.LIFECYCLE_QUEUE);
  }

  private static void ensureArchiveSchedule(WorkflowServiceStubs service) {
    ScheduleClient schedules = ScheduleClient.newInstance(service, ScheduleClientOptions.newBuilder()
        .setNamespace(TemporalConnection.namespace())
        .setDataConverter(TemporalConnection.dataConverter())
        .build());
    Schedule schedule = Schedule.newBuilder()
        .setAction(ScheduleActionStartWorkflow.newBuilder()
            .setWorkflowType(Names.ARCHIVE)
            .setArguments(ArchiveRequest.newBuilder().setClosedMoreThanDays(Env.getInt("ARCHIVE_AFTER_DAYS", 30)).build())
            .setOptions(WorkflowOptions.newBuilder().setWorkflowId("claims-archive").setTaskQueue(Names.LIFECYCLE_QUEUE).build())
            .build())
        .setSpec(ScheduleSpec.newBuilder().setCronExpressions(List.of(Env.get("ARCHIVE_CRON", "0 2 * * *"))).build())
        .build();
    try {
      schedules.createSchedule(Names.ARCHIVE_SCHEDULE, schedule, ScheduleOptions.newBuilder().build());
      log.info("Created schedule {}", Names.ARCHIVE_SCHEDULE);
    } catch (ScheduleAlreadyRunningException exists) {
      log.info("Schedule {} exists", Names.ARCHIVE_SCHEDULE);
    }
  }
}
