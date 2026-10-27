package com.example.claims.temporal;

import com.example.claims.temporal.workflows.lifecycle.ClaimLifecycleWorkflowImpl;
import com.example.claims.temporal.workflows.stp.StpWorkflowImpl;
import com.example.claims.temporal.workflows.stp.VendorDispatchWorkflowImpl;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Replays real histories recorded from the docker-compose stack against the CURRENT workflow code.
 * If a code change (including a change to the flow interpreter) is not replay-compatible, this
 * fails with a non-determinism error instead of production workflows getting stuck.
 *
 * Add a history whenever a new path matters:
 *   temporal workflow show --workflow-id 'claim:CLM-1001-XXXX:stp:2' --output json > src/test/resources/histories/NAME.json
 * In CI, also replay a sample of recent production histories (fetched with the client) before deploying.
 */
class ReplayTest {
  private TestWorkflowEnvironment env;

  @BeforeEach
  void setUp() {
    env = TestEnvs.create();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  private static WorkflowExecutionHistory history(String name) throws Exception {
    try (var in = Objects.requireNonNull(ReplayTest.class.getResourceAsStream("/histories/" + name))) {
      return WorkflowExecutionHistory.fromJson(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void stpFlowInterpreterReplays() throws Exception {
    Worker worker = env.newWorker("replay-stp");
    worker.registerWorkflowImplementationTypes(StpWorkflowImpl.class, VendorDispatchWorkflowImpl.class);
    WorkflowReplayer.replayWorkflowExecution(history("stp-auto-default-v1.json"), worker);
  }

  @Test
  void lifecycleWithRetriageAndAdjusterUpdateReplays() throws Exception {
    Worker worker = env.newWorker("replay-lifecycle");
    worker.registerWorkflowImplementationTypes(ClaimLifecycleWorkflowImpl.class);
    WorkflowReplayer.replayWorkflowExecution(history("lifecycle-retriage-manual.json"), worker);
  }
}
