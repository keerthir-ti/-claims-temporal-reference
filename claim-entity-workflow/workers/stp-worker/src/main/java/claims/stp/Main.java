package claims.stp;

import claims.runtime.Env;
import claims.runtime.Names;
import claims.runtime.activities.ClaimsProActivities;
import claims.runtime.db.Database;
import claims.runtime.stage.StageVersionCheck;
import claims.runtime.temporal.TemporalConnection;
import claims.runtime.temporal.WorkerSettings;
import claims.stp.activities.StageCheckedActivities;
import claims.stp.flows.FlowCatalog;
import claims.stp.mock.MockClaimsProActivities;
import claims.stp.mock.MockCommHubActivities;
import claims.stp.mock.MockEClaimsActivities;
import claims.stp.mock.MockPolicyGateActivities;
import claims.stp.mock.MockDataStore;
import io.temporal.client.WorkflowClient;
import io.temporal.common.VersioningBehavior;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STP worker image (activities only), run as two deployments:
 *
 * <pre>
 * stp        claims-stp: Comm Hub, Policy Gate, eClaims and flow activities
 * claimspro  claimspro-activities: every ClaimsPro call, with the queue rate limit (CLAIMSPRO_RATE_PER_SECOND)
 * </pre>
 *
 * The STP logic itself runs inside ClaimWorkflow. Replacing mocks: construct the real implementations where the
 * Mock* classes are created below. The stage-version check (StageCheckedActivities) wraps whatever is registered.
 */
public final class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  private Main() {}

  public static void main(String[] args) {
    String role = args.length > 0 ? args[0] : "stp";
    WorkflowClient client = TemporalConnection.client(TemporalConnection.service());
    StageVersionCheck fence = new StageVersionCheck(Database.pool("PROJECTION", Env.getInt("DB_POOL_SIZE", 10)));
    MockDataStore mock = new MockDataStore(Database.pool("MOCK", Env.getInt("DB_POOL_SIZE", 10)));
    WorkerFactory factory = WorkerFactory.newInstance(client);

    switch (role) {
      case "stp" -> {
        Worker w = factory.newWorker(Names.STP_QUEUE,
            WorkerSettings.options(Names.STP_DEPLOYMENT, VersioningBehavior.PINNED).build());
        w.registerActivitiesImplementations(
            StageCheckedActivities.commHub(new MockCommHubActivities(mock), fence),
            new MockPolicyGateActivities(mock),
            StageCheckedActivities.eClaims(new MockEClaimsActivities(mock), fence),
            new FlowCatalog(Path.of(Env.get("FLOWS_DIR", "config/stp-flows"))));
      }
      case "claimspro" -> {
        ClaimsProActivities claimsPro = StageCheckedActivities.claimsPro(new MockClaimsProActivities(mock), fence);
        Worker w = factory.newWorker(Names.CLAIMSPRO_QUEUE,
            WorkerSettings.options(Names.CLAIMSPRO_DEPLOYMENT, VersioningBehavior.PINNED)
                // Server-enforced across all pollers of the queue: ClaimsPro's agreed request rate.
                .setMaxTaskQueueActivitiesPerSecond(Env.getDouble("CLAIMSPRO_RATE_PER_SECOND", 50))
                .build());
        w.registerActivitiesImplementations(claimsPro);
      }
      default -> throw new IllegalArgumentException("Role must be stp or claimspro, not " + role);
    }
    factory.start();
    log.info("STP worker image running as {}", role);
  }
}
