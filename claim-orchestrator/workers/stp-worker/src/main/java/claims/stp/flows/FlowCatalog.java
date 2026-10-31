package claims.stp.flows;

import claims.contracts.v1.ClaimSnapshot;
import claims.contracts.v1.FlowDefinition;
import claims.stp.activities.FlowActivities;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * STP flow definitions from config/flows/*.yaml (produced from the variations tool). Only registered
 * steps are allowed, in a valid order; every file is validated at worker start. The most specific match
 * on line of business, company, channel and state wins. Real implementation: keep it.
 */
public class FlowCatalog implements FlowActivities {
  public static final List<String> STEPS = List.of("CONTACT_CUSTOMER", "VERIFY_COVERAGE", "OPEN_RESERVE",
      "DISPATCH_AND_APPRAISAL", "AWAIT_ESTIMATE", "PAY", "CLOSE_CLAIM");
  private static final Set<String> MATCH_KEYS = Set.of("line_of_business", "company", "channel", "state");

  private record Entry(FlowDefinition flow, Map<String, String> match) {}

  private final List<Entry> entries = new ArrayList<>();

  public FlowCatalog(Path dir) {
    ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : files.filter(p -> p.toString().endsWith(".yaml")).sorted().toList()) {
        byte[] bytes = Files.readAllBytes(f);
        JsonNode n = yaml.readTree(bytes);
        List<String> steps = new ArrayList<>();
        n.get("steps").forEach(s -> steps.add(s.asText()));
        validate(f, steps);
        Map<String, String> match = new java.util.HashMap<>();
        n.path("match").fields().forEachRemaining(e -> {
          if (!MATCH_KEYS.contains(e.getKey())) {
            throw new IllegalArgumentException(f + ": unknown match key " + e.getKey());
          }
          match.put(e.getKey(), e.getValue().asText());
        });
        entries.add(new Entry(FlowDefinition.newBuilder()
            .setFlowId(n.get("flow_id").asText())
            .setVersion(n.get("version").asInt())
            .setContentHash(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).substring(0, 12))
            .addAllSteps(steps)
            .setEstimateReviewCents(n.path("estimate_review_cents").asLong(0))
            .setEstimateRecheckSeconds(n.path("estimate_recheck_seconds").asInt(60))
            .build(), match));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    if (entries.isEmpty()) {
      throw new IllegalStateException("No STP flow definitions in " + dir);
    }
  }

  private static void validate(Path f, List<String> steps) {
    for (String s : steps) {
      if (!STEPS.contains(s)) {
        throw new IllegalArgumentException(f + ": step " + s + " is not registered");
      }
    }
    if (steps.contains("PAY") && steps.indexOf("PAY") < steps.indexOf("AWAIT_ESTIMATE")) {
      throw new IllegalArgumentException(f + ": PAY needs AWAIT_ESTIMATE before it");
    }
    if (!steps.isEmpty() && steps.contains("CLOSE_CLAIM") && !steps.get(steps.size() - 1).equals("CLOSE_CLAIM")) {
      throw new IllegalArgumentException(f + ": CLOSE_CLAIM must be the last step");
    }
  }

  @Override
  public FlowDefinition resolve(ClaimSnapshot claim) {
    Map<String, String> attrs = Map.of("line_of_business", claim.getLineOfBusiness(), "company", claim.getCompany(),
        "channel", claim.getChannel(), "state", claim.getState());
    Entry best = null;
    for (Entry e : entries) {
      boolean matches = e.match().entrySet().stream().allMatch(m -> m.getValue().equalsIgnoreCase(attrs.get(m.getKey())));
      if (matches && (best == null || e.match().size() > best.match().size())) {
        best = e;
      }
    }
    if (best == null) {
      throw ApplicationFailure.newNonRetryableFailure("No STP flow for " + attrs, "Refused");
    }
    return best.flow();
  }
}
