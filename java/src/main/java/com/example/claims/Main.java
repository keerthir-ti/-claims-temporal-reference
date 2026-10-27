package com.example.claims;

import com.example.claims.temporal.activities.mock.MockSystemsClient;
import com.example.claims.temporal.client.EventBridge;
import com.example.claims.temporal.client.TemporalSupport;
import com.example.claims.temporal.workers.Workers;
import com.example.claims.util.Env;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerFactory;
import java.util.Arrays;
import java.util.List;

/**
 * Java application entry point. One jar, several deployables; each role is its own container so
 * tracks scale and release independently:
 *
 * <pre>
 *   java -jar app.jar lifecycle   # ClaimLifecycleWorkflow + projection activities
 *   java -jar app.jar stp         # StpWorkflow + VendorDispatchWorkflow + STP activities
 *   java -jar app.jar bridge      # Claims Event Bridge (HTTP -> Signal / Signal-With-Start)
 *   java -jar app.jar lifecycle stp bridge   # everything in one process (local dev)
 * </pre>
 */
public final class Main {
  public static void main(String[] args) throws Exception {
    List<String> roles = args.length > 0 ? Arrays.asList(args) : Arrays.asList(Env.get("ROLES", "lifecycle,stp,bridge").split(","));
    WorkflowClient client = TemporalSupport.connect();
    WorkerFactory factory = WorkerFactory.newInstance(client);
    MockSystemsClient systems = new MockSystemsClient();

    if (roles.contains("lifecycle")) {
      Workers.lifecycle(factory, systems);
    }
    if (roles.contains("stp")) {
      Workers.stp(factory, systems);
    }
    factory.start();

    if (roles.contains("bridge")) {
      new EventBridge(client).serve(Env.getInt("BRIDGE_PORT", 8085));
    }
  }
}
