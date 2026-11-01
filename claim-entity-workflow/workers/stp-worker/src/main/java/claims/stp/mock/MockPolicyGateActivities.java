package claims.stp.mock;

import claims.contracts.CoverageResult;
import claims.contracts.StepRequest;
import claims.runtime.activities.PolicyGateActivities;
import com.fasterxml.jackson.databind.JsonNode;

/** Mock Policy Gate over the mock schema. Replace with the real Policy Gate call; the interface stays. */
public class MockPolicyGateActivities implements PolicyGateActivities {
  private final MockDataStore mock;

  public MockPolicyGateActivities(MockDataStore mock) {
    this.mock = mock;
  }

  @Override
  public CoverageResult verifyCoverage(StepRequest request) {
    mock.maybeFail("Policy Gate");
    JsonNode b = mock.behaviour(request.getStep().getClaimId(), "policyGate");
    return CoverageResult.newBuilder()
        .setCovered(b.path("covered").asBoolean(true))
        .setReason(b.path("reason").asText("")).build();
  }
}
