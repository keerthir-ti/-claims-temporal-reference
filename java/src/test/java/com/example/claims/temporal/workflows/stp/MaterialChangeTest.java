package com.example.claims.temporal.workflows.stp;

import com.example.claims.temporal.activities.ClaimSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class MaterialChangeTest {
  static ClaimSnapshot snap(boolean injury, long estimate, String coverage) {
    return new ClaimSnapshot("C", 1, "AUTO", "MA", "PLY", "AGENT", "COLLISION", injury, estimate, true, coverage, "OPEN");
  }

  @Test
  void immaterialChangesAreIgnored() {
    assertNull(MaterialChange.detect(snap(false, 1000, "CONFIRMED"), snap(false, 1400, "CONFIRMED")));
  }

  @Test
  void materialChangesAreReported() {
    assertEquals("injury reported", MaterialChange.detect(snap(false, 1000, "CONFIRMED"), snap(true, 1000, "CONFIRMED")));
    assertEquals("estimated loss up more than 50%", MaterialChange.detect(snap(false, 1000, "CONFIRMED"), snap(false, 1600, "CONFIRMED")));
    assertEquals("coverage now UNCERTAIN", MaterialChange.detect(snap(false, 1000, "CONFIRMED"), snap(false, 1000, "UNCERTAIN")));
  }
}
