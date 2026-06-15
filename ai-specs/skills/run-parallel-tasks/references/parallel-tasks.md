# Parallel Feature Tasks

Fill in the tasks below and say **"run parallel-tasks.md"** to start, or provide explicit arguments like **"run parallel tasks SCRUM-83 SCRUM-84"**.  
When explicit arguments are provided, they override this file list.  
Each task runs in its own worktree through the full specboot pipeline (enrich → new → ff → apply → verify).  
Process stops after verify — no archive, no commit, no worktree cleanup.

## US source formats

| Format | When to use |
|--------|-------------|
| `us: inline` + `description:` block | You have the US text ready |
| `us: docs/us/my-feature.md` | US is in a markdown file in the repo |
| `us: PROJ-123` | US is a Jira ticket (enrich-us will fetch and enrich it) |

---

## Tasks

<!--
Copy a task block for each feature. Minimum required: name + us.
Comment out tasks you don't want to run yet.
-->

Task 1
tmp/SCRUM-81-adversarial-review.md

<!--
Task 1
SCRUM-83


Task 2
SCRUM-82
-->

<!--
### Task 1
name: item-tags
us: inline
description: |
  As a logged-in user, I want to tag and categorize items so that I can filter and group them easily.
  Items have no classification. Users can't filter or group anything. Tags would be a natural M:M extension
  and touch the full stack: new Tag model, junction table, filter UI, updated API.
-->

<!--
### Task 2
name: another-feature
us: docs/us/another-feature.md

### Task 3
name: third-feature
us: PROJ-456
-->
