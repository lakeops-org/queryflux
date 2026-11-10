---
slug: two-agents-one-config
title: "Two agents edited the same config. Both got a 200."
description: Agents now operate the data stack too. We gave two of them QueryFlux admin tokens, let them edit the same group, and watched one quietly undo the other.
authors: [amit]
tags: [Guides]
keywords:
  - AI agents
  - agentic operations
  - admin API
  - optimistic concurrency
  - ETag
  - If-Match
  - audit log
  - QueryFlux
image: img/blog/two-agents-one-config-hero.jpg
---

The first agents that touched your data stack only read from it. The next ones change it. One watches the queue and raises a group's concurrency limit when it backs up. Another works through access tickets ("give on-call access to the agents pool") and edits the allow-list. Neither one needs a human to click Save.

That's a reasonable thing to automate. It also brings back a bug most admin UIs have never had to deal with, because people rarely edit the same screen in the same second. Agents on a schedule do it all the time.

> **A person clicks Save once in a while. Two agents read, change and write the same config every few minutes, and sooner or later one of them writes over the other.**

We tried it on QueryFlux's admin API. Both agents' writes returned `200`, and one change disappeared. Here's what happened, what stopped it, and what the audit log did and didn't record.

<!-- truncate -->

## How we ran it

:::note
Scoped tokens, `If-Match` and the audit log are on `main` and will ship in the next release. They aren't in 0.4.0.
:::

We ran QueryFlux from `main` with Postgres persistence and a cluster group called `agents`, with `maxRunningQueries: 4`. Using the admin user, we created two tokens with the [scoped tokens](/docs/next/authentication#admin-api-access) added in [#373](https://github.com/lakeops-org/queryflux/pull/373):

- **`capacity-agent`**, which raises the group's concurrency when the queue grows;
- **`access-agent`**, which adds IdP groups to the allow-list.

As in [an earlier experiment](/blog/busiest-sql-user-has-amnesia), **we scripted the HTTP calls ourselves; no model made them.** The tokens, ETags, status codes and audit rows below are real responses from the API. The order of the calls is ours.

## 1. Give each agent the smallest key

A token is created once and shown once. QueryFlux stores only its SHA-256:

```bash
curl -s -u admin:$ADMIN_PASSWORD -X POST localhost:9000/admin/tokens \
  -H 'Content-Type: application/json' \
  -d '{"name": "capacity-agent", "scopes": ["read", "config:write"]}'
```

There are four scopes. `read` covers every `GET`. `operate` covers actions such as cancelling queries or disabling a cluster. `config:write` covers every other config change. `credentials` covers tokens and the admin password. We then had the capacity agent try three things it shouldn't do:

| The agent tries to | Response |
| --- | --- |
| create itself a token with `credentials` | `403 this needs the 'credentials' scope` |
| cancel someone's query | `403 this needs the 'operate' scope` |
| list the other tokens | `403 this needs the 'credentials' scope` |

A token can't grant scopes it doesn't hold, and `config:write` doesn't include `credentials`, so an agent can't promote itself. The error messages also say exactly what's missing. That matters, because [an agent acts on whatever the error tells it](/blog/busiest-sql-user-has-amnesia).

**Know how wide `config:write` is.** Scopes cover kinds of action, not individual resources. A token that can change `maxRunningQueries` can also change routing, guardrails and access control. If an agent should only ever touch one field, give it a narrow tool that makes that one call, and keep the token behind the tool.

## 2. The update that disappeared

Both agents read the group at the same time, and both saw `maxRunningQueries: 4` with `ETag: "r1"`. The capacity agent then raised the limit to 8. The access agent added `oncall` to the copy it had read earlier and sent that copy back:

```text
capacity-agent  GET  /admin/config/groups/agents   200  r1  max 4, allowGroups []
access-agent    GET  /admin/config/groups/agents   200  r1  max 4, allowGroups []
capacity-agent  PUT  maxRunningQueries = 8         200  r2  max 8
access-agent    PUT  allowGroups += oncall         200  r3  max 4, allowGroups [agents, oncall]
```

Both agents got a `200` and both reported success. But the group ended up back at **4**, because a group update is a `PUT` of the whole object. The access agent sent back the 4 it had read, along with its own change.

<figure class="blog-figure">
  <img src="/img/blog/agent-operators-if-match.svg" alt="Two timelines. Without If-Match, both agents read r1, both writes return 200, and the second write sets maxRunningQueries back from 8 to 4. With If-Match, the second write gets 412 with ETag r6; the agent reads again, writes with If-Match r6, and both changes are kept." />
  <figcaption>The same two edits, without and with <code>If-Match</code>. Revisions are the real ETags the API returned.</figcaption>
</figure>

Nothing failed, so nothing alerted. The queue the capacity agent was trying to drain keeps growing. Its next run sees 4 again, raises it to 8 again, and the access agent may well undo it again. To anyone reading the logs, it looks like an agent that keeps forgetting what it did.

## 3. If-Match: refuse the stale write

Every config read returns the config revision as an `ETag`. Send it back as `If-Match`, and the write only goes through if nothing has changed since you read:

```text
capacity-agent  GET  groups/agents                     200  r4  max 4
access-agent    GET  groups/agents                     200  r4  max 4
capacity-agent  PUT  If-Match: "r4"   max = 8          200  r6
access-agent    PUT  If-Match: "r4"   += oncall        412  ETag: r6
                     config changed since it was read; read it again
access-agent    GET  groups/agents                     200  r6  max 8
access-agent    PUT  If-Match: "r6"   += oncall        200  r8  max 8, allowGroups [agents, oncall]
```

Both changes were kept. The `412` also carries the current ETag, and the message tells the agent what to do next in words a model will follow. On the agent's side, the loop is short:

```python
def update_group(name, change, attempts=5):
    for _ in range(attempts):
        r = admin.get(f"/admin/config/groups/{name}")
        body = change(r.json())  # apply the edit to what's there now
        w = admin.put(f"/admin/config/groups/{name}", json=body,
                      headers={"If-Match": r.headers["ETag"]})
        if w.status_code != 412:
            w.raise_for_status()
            return w.json()
    raise RuntimeError(f"{name} kept changing; giving up without writing")
```

The important part is that `change` runs on the *fresh* copy each time. Retrying with the body from before the `412` just produces the lost update again, with more steps.

Two things about the ETag that surprised us:

- **It's one revision for all of config.** We had the access agent read the routing config at `r8`, then let the capacity agent edit the group. The access agent's `PUT` of the routing config it had read, completely unchanged, got a `412` at `r10`, even though it touched something different. You'll see more retries than real conflicts, but you'll never get a silent merge.
- **It can go up by more than one per write.** A conditional write claims the revision and then writes, so we saw `r4` jump to `r6`. Never work out the next ETag yourself. Use the one the response gives you.

`If-Match` is optional. A write without it behaves as before, unconditionally. The server can't make your agents use it, so build it into whatever tool or client library they write through.

## 4. What the audit log saw

Every successful write was there, attributed to the token that made it:

```text
basic:admin             POST  /admin/tokens                 201
basic:admin             POST  /admin/tokens                 201
basic:admin             PUT   /admin/config/groups/agents   200
token:025a7004-1a06-4d  PUT   /admin/config/groups/agents   200
token:06d20609-742c-4c  PUT   /admin/config/groups/agents   200
...
```

That's enough to answer *who wrote to this group, and when*. It isn't enough to answer *what changed*. The row for the access agent's write in step 2, the one that put 4 back, looks the same as any other successful `PUT`. The log records method, path and status, not the body or a diff.

The log was also missing some requests: the three `403`s from step 1 and both `412`s. Our docs say every request needing a scope other than `read` is recorded along with its status, but refused requests return before the audit write. An agent repeatedly trying to create itself a `credentials` token is exactly what you'd want an audit log to show.

## What the run caught in our own stack

| What we found | Evidence | What to do for now |
| --- | --- | --- |
| **Refused admin requests aren't audited.** `403` and `412` responses return before the audit write. | 3 denials and 2 conflicts, none in `GET /admin/audit`. | Log refused requests in the agent's own tooling, or at a gateway in front of the admin port. |
| **The audit log has no diff.** | The write that reverted `maxRunningQueries` looks like any other `200`. | Have the agent tooling record the before and after bodies alongside the response. |
| **Rows name the token by id.** | `token:025a7004-…`. Turning an id into a name needs `GET /admin/tokens`, which needs `credentials`. | Keep a list of token ids and agent names when you create tokens. |
| **`If-Match` is opt-in.** | Writes without it are unconditional. | Send it from every agent tool, and treat a write without it as a bug. |

## Before an agent gets an admin token

1. **Give each agent its own token, and only the scopes it needs.** Separate tokens are the only way to tell agents apart in the audit log, and to revoke one without the others.
2. **Wrap broad scopes in narrow tools.** `config:write` covers all config, so if the agent's job is one field, expose one field.
3. **Always send `If-Match`, and reapply the change after a `412`.** Read again and redo the change. Never resend the old body.
4. **Keep your own record of changes.** Until the audit log stores diffs and refusals, the agent's tooling is the only place with the full story.
5. **Revoke quickly.** `DELETE /admin/tokens/{id}` takes effect on the next request. Make sure the person on call knows that.

Agents that change config aren't more dangerous than people who change config. They're faster, they run at the same time, and they don't notice when something looks off. The proxy has to catch the conflict, because the agents won't.

<div class="blog-cta">

Read [admin API access](/docs/next/authentication#admin-api-access) for scopes, tokens, ETags and the audit log, and [give agents SQL, not the keys](/blog/agents-sql-not-keys) for the query side of the same problem.

</div>
