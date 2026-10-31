package claims.stp;

import claims.stp.workflows.PaymentWorkflowImpl;
import claims.stp.workflows.StpWorkflowImpl;
import io.temporal.testing.WorkflowReplayer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Replays recorded histories (src/test/resources/histories) against the current code. A change that
 * breaks determinism for workflows already running fails here, before release. Refresh the files from
 * a test environment with: temporal workflow show -w WORKFLOW_ID -o json
 */
class ReplayTest {
  @Test
  void recordedHistoriesReplay() throws Exception {
    Path dir = Path.of("src/test/resources/histories");
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList()) {
        Class<?> type = f.getFileName().toString().startsWith("payment") ? PaymentWorkflowImpl.class : StpWorkflowImpl.class;
        WorkflowReplayer.replayWorkflowExecution(Files.readString(f), type);
      }
    }
  }
}
