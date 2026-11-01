# Claims Orchestration on Temporal

Two production approaches for the claims platform's Temporal layer, each built and runnable on its own. Both use
only the components in the client's architecture (ClaimsPro, eClaims, ImageRight, Policy Gate, Comm Hub, the event
bridge, Kong, Temporal Cloud, EKS workers in Java and Python, the projection table, S3), mock the claims systems at
the activity boundary only, and share the same production baseline: application data outside Temporal with an S3
archive, Protocol Buffers contracts, stage-version checks and idempotency keys, a rate-limited ClaimsPro queue,
payload encryption, the full approval rules, and Worker Versioning.

| Folder | Approach | Temporal pattern | In one line |
|---|---|---|---|
| [claim-orchestrator/](claim-orchestrator/) | A: Parent–Child Orchestration | Hierarchical workflows | A claim workflow with a child workflow per track visit and a payment child; each track team owns its workflow |
| [claim-entity-workflow/](claim-entity-workflow/) | B: Claim Entity Workflow | Entity workflow | One workflow per claim; triage, investigation and STP are modules inside it, their work runs as activities on each track's own workers |

Choose A when separate teams must own and release their track workflows independently. Choose B when one team owns
the claim process and simplicity, cost and one view of the claim matter most. The comparison is in each approach's
`design/` document.

Each folder has its own README and is started with `make up` and `make demo` from inside it.
