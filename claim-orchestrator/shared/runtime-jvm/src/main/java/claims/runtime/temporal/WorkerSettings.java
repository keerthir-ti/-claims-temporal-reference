package claims.runtime.temporal;

import claims.runtime.Env;
import io.temporal.common.VersioningBehavior;
import io.temporal.common.WorkerDeploymentVersion;
import io.temporal.worker.WorkerDeploymentOptions;
import io.temporal.worker.WorkerOptions;

/**
 * Worker options with Worker Versioning. Every worker announces {deployment, BUILD_ID}; a new build
 * receives work only after it is made current (deploy/promote-versions.sh locally, the release
 * pipeline in production, normally after a ramp).
 */
public final class WorkerSettings {
  private WorkerSettings() {}

  public static WorkerOptions.Builder options(String deployment, VersioningBehavior defaultBehavior) {
    WorkerOptions.Builder options = WorkerOptions.newBuilder()
        .setMaxConcurrentActivityExecutionSize(Env.getInt("MAX_CONCURRENT_ACTIVITIES", 100))
        .setMaxConcurrentWorkflowTaskExecutionSize(Env.getInt("MAX_CONCURRENT_WORKFLOW_TASKS", 100));
    if (Env.getBool("WORKER_VERSIONING", true)) {
      options.setDeploymentOptions(WorkerDeploymentOptions.newBuilder()
          .setUseVersioning(true)
          .setVersion(new WorkerDeploymentVersion(deployment, Env.get("BUILD_ID", "dev")))
          .setDefaultVersioningBehavior(defaultBehavior)
          .build());
    }
    return options;
  }
}
