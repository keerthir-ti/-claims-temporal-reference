package claims.lifecycle;

import claims.lifecycle.workflows.ClaimWorkflowImpl;
import io.temporal.testing.WorkflowReplayer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Replays recorded ClaimWorkflow histories against the current code. The parent runs on
 * Auto-Upgrade, so every change must replay these; patch (Workflow.getVersion) when it would not.
 */
class ReplayTest {
  @Test
  void recordedHistoriesReplay() throws Exception {
    try (Stream<Path> files = Files.list(Path.of("src/test/resources/histories"))) {
      for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList()) {
        WorkflowReplayer.replayWorkflowExecution(Files.readString(f), ClaimWorkflowImpl.class);
      }
    }
  }
}
