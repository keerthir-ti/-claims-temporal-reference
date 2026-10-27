package com.example.claims.temporal.activities.mock;

import com.example.claims.contracts.Track;
import com.example.claims.temporal.activities.LifecycleActivities;
import java.util.Map;

/** MOCK of the lifecycle activities against ClaimsPro. Replace with the real ClaimsPro calls. */
public class MockLifecycleActivities implements LifecycleActivities {
  private final MockSystemsClient systems;

  public MockLifecycleActivities(MockSystemsClient systems) {
    this.systems = systems;
  }

  @Override
  public void beginStage(String claimId, int stageVersion, Track track) {
    systems.post("/claimspro/claims/" + claimId + "/stage", Map.of("stageVersion", stageVersion, "track", track.value()));
  }

  @Override
  public int currentStageVersion(String claimId) {
    return systems.get("/claimspro/claims/" + claimId).path("currentStageVersion").asInt(0);
  }
}
