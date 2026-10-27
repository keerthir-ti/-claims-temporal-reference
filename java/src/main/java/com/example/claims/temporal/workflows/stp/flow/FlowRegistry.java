package com.example.claims.temporal.workflows.stp.flow;

import com.example.claims.temporal.activities.ClaimSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Loads and validates flow definitions, then picks the most specific one for a claim. Runs inside
 * an activity (file IO is not allowed in workflow code).
 *
 * <p>Selection: every key in a flow's {@code match} must equal the claim's value; among matches the
 * flow with the most keys wins (auto+TX+CAT beats auto+TX beats auto). Equal specificity is a
 * configuration error caught at load time.
 */
public class FlowRegistry {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

  private final List<FlowDefinition> flows;

  public FlowRegistry(List<FlowDefinition> flows) {
    this.flows = List.copyOf(flows);
    for (FlowDefinition a : flows) {
      for (FlowDefinition b : flows) {
        if (a != b && a.match().equals(b.match())) {
          throw new IllegalStateException("ambiguous flows " + a.ref() + " and " + b.ref());
        }
      }
    }
  }

  public static FlowRegistry load(Path dir) {
    List<FlowDefinition> loaded = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path p : files.filter(f -> f.toString().endsWith(".yaml")).sorted().toList()) {
        loaded.add(parse(Files.readAllBytes(p), p.getFileName().toString()));
      }
    } catch (IOException e) {
      throw new IllegalStateException("cannot read flows from " + dir, e);
    }
    return new FlowRegistry(loaded);
  }

  static FlowDefinition parse(byte[] yaml, String source) throws IOException {
    FlowDefinition raw = YAML.readValue(yaml, FlowDefinition.class);
    List<String> errors = new ArrayList<>();
    if (raw.id() == null || raw.version() <= 0) {
      errors.add("id and positive version are required");
    }
    if (raw.stages() == null || raw.stages().isEmpty()) {
      errors.add("at least one stage is required");
    } else {
      for (FlowDefinition.Stage s : raw.stages()) {
        for (String step : s.steps()) {
          if (!FlowInvariants.REGISTERED_STEPS.contains(step)) {
            errors.add("stage " + s.name() + ": unregistered step " + step);
          }
        }
      }
      List<String> all = raw.stages().stream().flatMap(s -> s.steps().stream()).toList();
      if (all.contains("FINALIZE_PAYMENT") && !all.contains("ISSUE_PAYMENT")) {
        errors.add("FINALIZE_PAYMENT requires ISSUE_PAYMENT");
      }
      if (all.contains("ISSUE_PAYMENT") && all.indexOf("AWAIT_ESTIMATE") > all.indexOf("ISSUE_PAYMENT")) {
        errors.add("ISSUE_PAYMENT must come after AWAIT_ESTIMATE");
      }
    }
    if (!errors.isEmpty()) {
      throw new IllegalStateException(source + ": " + String.join("; ", errors));
    }
    return new FlowDefinition(raw.id(), raw.version(), sha256(yaml),
        raw.match() == null ? Map.of() : raw.match(), raw.authorityLimitCents(), raw.stages());
  }

  public FlowDefinition select(ClaimSnapshot c) {
    Map<String, String> attrs = Map.of(
        "lob", nz(c.lob()), "state", nz(c.state()), "company", nz(c.company()),
        "routingChannel", nz(c.routingChannel()));
    FlowDefinition best = null;
    for (FlowDefinition f : flows) {
      boolean matches = f.match().entrySet().stream()
          .allMatch(e -> e.getValue().equalsIgnoreCase(attrs.getOrDefault(e.getKey(), "")));
      if (matches && (best == null || f.match().size() > best.match().size())) {
        best = f;
      }
    }
    if (best == null) {
      throw new IllegalStateException("no STP flow for " + attrs);
    }
    return best;
  }

  private static String nz(String s) {
    return s == null ? "" : s;
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).substring(0, 16);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
