package com.example.claims.temporal.workers;

import com.example.claims.db.InMemoryProjectionStore;
import com.example.claims.db.JdbcProjectionStore;
import com.example.claims.db.ProjectionStore;
import com.example.claims.temporal.Names;
import com.example.claims.temporal.activities.ProjectionActivitiesImpl;
import com.example.claims.temporal.activities.mock.MockLifecycleActivities;
import com.example.claims.temporal.activities.mock.MockStpActivities;
import com.example.claims.temporal.activities.mock.MockSystemsClient;
import com.example.claims.temporal.workflows.lifecycle.ClaimLifecycleWorkflowImpl;
import com.example.claims.temporal.workflows.stp.StpWorkflowImpl;
import com.example.claims.temporal.workflows.stp.VendorDispatchWorkflowImpl;
import com.example.claims.temporal.workflows.stp.flow.FlowRegistry;
import com.example.claims.util.Env;
import io.temporal.common.VersioningBehavior;
import io.temporal.common.WorkerDeploymentVersion;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerDeploymentOptions;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker registration per deployment. Swapping mock activities for real ones happens here: replace
 * {@code Mock*Activities} with the real implementations of the same interfaces.
 */
public final class Workers {
  private static final Logger log = LoggerFactory.getLogger(Workers.class);

  private Workers() {}

  /** Lifecycle deployment: the parent workflow + the (real) projection-write activity. */
  public static void lifecycle(WorkerFactory factory, MockSystemsClient systems) {
    // Long-lived parent: moves to new code (AUTO_UPGRADE), kept replay-safe with getVersion + replay tests.
    WorkerOptions options = options(Names.LIFECYCLE_DEPLOYMENT, VersioningBehavior.AUTO_UPGRADE);
    Worker lifecycle = factory.newWorker(Names.LIFECYCLE_QUEUE, options);
    lifecycle.registerWorkflowImplementationTypes(ClaimLifecycleWorkflowImpl.class);
    lifecycle.registerActivitiesImplementations(new MockLifecycleActivities(systems));

    Worker projection = factory.newWorker(Names.PROJECTION_QUEUE, options);
    projection.registerActivitiesImplementations(new ProjectionActivitiesImpl(projectionStore()));
  }

  /** STP deployment: STP track + vendor-dispatch grandchild. */
  public static void stp(WorkerFactory factory, MockSystemsClient systems) {
    Path flowsDir = Path.of(Env.get("FLOWS_DIR", "../config/flows"));
    FlowRegistry.load(flowsDir); // fail fast on an invalid flow at startup
    // Short-lived track children stay on the code they started with (PINNED).
    Worker stp = factory.newWorker(Names.STP_QUEUE, options(Names.STP_DEPLOYMENT, VersioningBehavior.PINNED));
    stp.registerWorkflowImplementationTypes(StpWorkflowImpl.class, VendorDispatchWorkflowImpl.class);
    stp.registerActivitiesImplementations(new MockStpActivities(systems, () -> FlowRegistry.load(flowsDir)));
  }

  /**
   * Worker Deployment Versioning: every worker announces {deployment, BUILD_ID}. A new version only
   * receives work once promoted with `temporal worker deployment set-current-version` (the
   * deploy-setup job locally, the release pipeline in production).
   */
  static WorkerOptions options(String deployment, VersioningBehavior defaultBehavior) {
    WorkerOptions.Builder options = WorkerOptions.newBuilder()
        .setMaxConcurrentActivityExecutionSize(Env.getInt("MAX_CONCURRENT_ACTIVITIES", 200))
        .setMaxConcurrentWorkflowTaskExecutionSize(Env.getInt("MAX_CONCURRENT_WORKFLOW_TASKS", 200));
    if (Env.getBool("WORKER_VERSIONING", true)) {
      String buildId = Env.get("BUILD_ID", "dev");
      options.setDeploymentOptions(WorkerDeploymentOptions.newBuilder()
          .setUseVersioning(true)
          .setVersion(new WorkerDeploymentVersion(deployment, buildId))
          .setDefaultVersioningBehavior(defaultBehavior)
          .build());
      log.info("Worker deployment {} build {} default behaviour {}", deployment, buildId, defaultBehavior);
    }
    return options.build();
  }

  private static ProjectionStore projectionStore() {
    String url = Env.get("PROJECTION_JDBC_URL", "");
    if (url.isEmpty()) {
      return new InMemoryProjectionStore();
    }
    return new JdbcProjectionStore(url, Env.get("PROJECTION_DB_USER", "claims"), Env.get("PROJECTION_DB_PASSWORD", "claims"));
  }
}
