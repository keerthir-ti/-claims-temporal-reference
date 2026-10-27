package com.example.claims.temporal;

import com.example.claims.temporal.client.ContractJson;
import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;

public final class TestEnvs {
  private TestEnvs() {}

  /**
   * Time-skipping test server with the same data converter and Search Attributes as production.
   * Without the attributes every upsert is rejected and the workflow task retries forever, exactly
   * what happens in a real namespace where they were never registered.
   */
  public static TestWorkflowEnvironment create() {
    TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder()
        .setWorkflowClientOptions(WorkflowClientOptions.newBuilder().setDataConverter(ContractJson.dataConverter()).build())
        .build());
    env.registerSearchAttribute("ClaimId", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    env.registerSearchAttribute("ClaimTrack", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    env.registerSearchAttribute("StpFlow", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
    env.registerSearchAttribute("StageVersion", IndexedValueType.INDEXED_VALUE_TYPE_INT);
    env.registerSearchAttribute("RetriageCount", IndexedValueType.INDEXED_VALUE_TYPE_INT);
    return env;
  }
}
