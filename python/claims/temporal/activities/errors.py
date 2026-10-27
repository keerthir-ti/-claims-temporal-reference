"""Non-retryable failure types, identical to IntegrationErrors.java. Anything else is retried by Temporal."""
STALE_STAGE = "StaleStage"          # 409: write from an abandoned stage
NOT_FOUND = "ClaimNotFound"         # 404
BUSINESS_RULE = "BusinessRuleViolation"  # 422: the system refused; retrying will not help
