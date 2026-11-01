package claims.stp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import claims.contracts.ClaimSnapshot;
import claims.stp.workflows.MaterialChange;
import org.junit.jupiter.api.Test;

/** Which ClaimsPro changes send a claim in STP back to triage. */
class MaterialChangeTest {
  private static final ClaimSnapshot BEFORE = ClaimSnapshot.newBuilder().setClaimId("C1").setStatus("OPEN")
      .setLineOfBusiness("AUTO").setState("OH").setClaimedAmountCents(400_000).setChangeVersion(1).build();

  @Test
  void cosmeticChangesAreNotMaterial() {
    assertNull(MaterialChange.detect(BEFORE, BEFORE.toBuilder().setChangeVersion(7).setAdjusterId("adj-2").build()));
    assertNull(MaterialChange.detect(BEFORE, BEFORE.toBuilder().setClaimedAmountCents(500_000).build()));
  }

  @Test
  void materialChangesAreNamed() {
    assertEquals("injury reported", MaterialChange.detect(BEFORE, BEFORE.toBuilder().setInjuryReported(true).build()));
    assertEquals("line of business changed", MaterialChange.detect(BEFORE, BEFORE.toBuilder().setLineOfBusiness("HOME").build()));
    assertEquals("claimed amount up by more than half", MaterialChange.detect(BEFORE, BEFORE.toBuilder().setClaimedAmountCents(700_000).build()));
  }
}
