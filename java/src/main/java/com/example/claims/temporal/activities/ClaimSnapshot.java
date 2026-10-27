package com.example.claims.temporal.activities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Current claim data as returned by ClaimsPro (GET /claims/{id}). Lives only inside activities and
 * workflow memory; it is never part of a cross-language contract (children re-read ClaimsPro themselves).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ClaimSnapshot(
    String claimId,
    long version,
    String lob,
    String state,
    String company,
    String routingChannel,
    String lossType,
    boolean injuryReported,
    long estimatedLossCents,
    boolean policyActive,
    String coverageStatus,
    String status) {}
