package claims.lifecycle.activities;

import claims.contracts.DecisionPolicy;
import claims.contracts.PolicyRequest;
import claims.runtime.Env;
import claims.runtime.activities.PolicyActivities;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Path;

/**
 * Decision roles, deadlines and limits from config/decision-policy.yaml. Real implementation; the
 * file is replaced by the client's authority matrix (or this activity reads it from where it lives).
 */
public class PolicyActivitiesImpl implements PolicyActivities {
  private final Path file;

  public PolicyActivitiesImpl(Path file) {
    this.file = file;
    load(PolicyRequest.getDefaultInstance()); // fail fast on an invalid file
  }

  @Override
  public DecisionPolicy load(PolicyRequest request) {
    try {
      String json = new ObjectMapper().writeValueAsString(new ObjectMapper(new YAMLFactory()).readTree(file.toFile()));
      DecisionPolicy.Builder policy = DecisionPolicy.newBuilder();
      JsonFormat.parser().merge(json, policy);
      int window = Env.getInt("RETRY_WINDOW_SECONDS", 0); // shorter window for local runs
      if (window > 0) {
        policy.setRetryWindowSeconds(window);
      }
      if (Env.getBool("PILOT_STP_ONLY", false)) { // pilot scope: Claims Lifecycle -> STP only
        policy.setPilotStpOnly(true);
      }
      return policy.build();
    } catch (Exception e) {
      throw new IllegalStateException("Invalid decision policy " + file, e);
    }
  }
}
