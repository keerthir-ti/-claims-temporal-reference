package com.example.claims.temporal.workflows.stp.flow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * An immutable, validated STP flow (config/flows/*.yaml). The workflow receives it as an activity
 * result, so the exact definition a claim ran with is in its Event History: editing the YAML never
 * changes a running claim or breaks replay.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FlowDefinition(
    String id,
    int version,
    String contentHash,
    Map<String, String> match,
    long authorityLimitCents,
    List<Stage> stages) {

  /** Steps inside a stage run in parallel; stages run in order. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Stage(String name, Boolean interruptible, List<String> steps) {}

  public String ref() {
    return id + "@" + version;
  }
}
