package com.example.claims.temporal;

import io.temporal.common.SearchAttributeKey;

/**
 * Custom Search Attributes (registered on the namespace; see docker-compose.yml / docs).
 * Values are operational, non-PII identifiers only.
 */
public final class SearchAttrs {
  private SearchAttrs() {}

  public static final SearchAttributeKey<String> CLAIM_ID = SearchAttributeKey.forKeyword("ClaimId");
  public static final SearchAttributeKey<String> CLAIM_TRACK = SearchAttributeKey.forKeyword("ClaimTrack");
  public static final SearchAttributeKey<Long> STAGE_VERSION = SearchAttributeKey.forLong("StageVersion");
  public static final SearchAttributeKey<Long> RETRIAGE_COUNT = SearchAttributeKey.forLong("RetriageCount");
  public static final SearchAttributeKey<String> STP_FLOW = SearchAttributeKey.forKeyword("StpFlow");
}
