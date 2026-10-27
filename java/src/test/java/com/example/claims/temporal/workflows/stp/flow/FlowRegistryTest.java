package com.example.claims.temporal.workflows.stp.flow;

import com.example.claims.temporal.activities.ClaimSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class FlowRegistryTest {
  private static final Path FLOWS = Path.of(System.getProperty("flows.dir", "../config/flows"));

  public static ClaimSnapshot claim(String lob, String state, String channel) {
    return new ClaimSnapshot("C", 1, lob, state, "PLY", channel, "X", false, 1000, true, "CONFIRMED", "OPEN");
  }

  @Test
  void mostSpecificFlowWins() {
    FlowRegistry registry = FlowRegistry.load(FLOWS);
    assertEquals("auto-cat-tx", registry.select(claim("AUTO", "TX", "CAT")).id());
    assertEquals("auto-default", registry.select(claim("AUTO", "TX", "AGENT")).id());
    assertEquals("home-default", registry.select(claim("HOME", "RI", "AGENT")).id());
  }

  @Test
  void contentHashIsPinned() {
    FlowDefinition f = FlowRegistry.load(FLOWS).select(claim("AUTO", "MA", "AGENT"));
    assertEquals(16, f.contentHash().length());
  }

  @Test
  void unregisteredStepsAndBadOrderingAreRejected(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("bad.yaml"), """
        id: bad
        version: 1
        match: { lob: AUTO }
        authorityLimitCents: 100
        stages:
          - name: pay
            steps: [ISSUE_PAYMENT]
          - name: oops
            steps: [AWAIT_ESTIMATE, RUN_SQL]
        """);
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> FlowRegistry.load(dir));
    assertTrue(e.getMessage().contains("unregistered step RUN_SQL"), e.getMessage());
    assertTrue(e.getMessage().contains("ISSUE_PAYMENT must come after AWAIT_ESTIMATE"), e.getMessage());
  }

  @Test
  void invariantsOverrideYaml() {
    FlowDefinition.Stage pay = new FlowDefinition.Stage("pay", true, java.util.List.of("ISSUE_PAYMENT"));
    assertEquals(false, FlowInvariants.interruptible(pay));
    FlowDefinition generous = new FlowDefinition("x", 1, "h", java.util.Map.of(), 99_000_000L, java.util.List.of(pay));
    assertEquals(FlowInvariants.MAX_AUTHORITY_CENTS, FlowInvariants.effectiveAuthority(generous));
  }
}
