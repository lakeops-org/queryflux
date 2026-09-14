---
sidebar_label: Access control (moved)
title: Data access control
description: Data access control docs moved to the Access Control section.
draft: false
unlisted: true
---
# Data access control

This page has moved into its own section:

- **[Access control overview](../access-control/overview)** — concepts, pipeline, config, rewrite mechanics
- **[OPA provider](../access-control/opa)** — Rego wire format, delegation patterns, local demo

They share one enforcement path: access control is the `opa_access` rewriting guard in the [guardrail chain](./guardrails). Studio's **Queries** page shows both on the same Guard Actions trail.

**Configuration today:** one or more named OPA connections (`accessControl.connections` — no name is reserved), a `defaultConnection` groups fall back to, a global `enabled` default, and per–cluster-group overrides — `enabled`, `failOpen`, and which connection to use — under `accessControl.groups`. Edited on Studio **Access Control** (connections, `defaultConnection`, and scope), not on **Clusters**. Policy (Rego) is never stored in QueryFlux. See [product model](../access-control/overview#product-model-what-queryflux-configures) and [multiple connections](../access-control/overview#multiple-connections).
