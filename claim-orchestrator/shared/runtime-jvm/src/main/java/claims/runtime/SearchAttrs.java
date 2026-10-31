package claims.runtime;

import io.temporal.common.SearchAttributeKey;

/** Search attributes. They are not encrypted by the payload codec, so they hold IDs and codes only. */
public final class SearchAttrs {
  private SearchAttrs() {}

  public static final SearchAttributeKey<String> CLAIM_ID = SearchAttributeKey.forKeyword("ClaimId");
  public static final SearchAttributeKey<String> STAGE = SearchAttributeKey.forKeyword("Stage");
  public static final SearchAttributeKey<String> TRACK = SearchAttributeKey.forKeyword("Track");
  public static final SearchAttributeKey<String> OPEN_DECISION = SearchAttributeKey.forKeyword("OpenDecision");
}
