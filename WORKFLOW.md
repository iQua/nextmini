---
You are working on a Linear ticket `{{ issue.identifier }}` for `Nextmini`.

{% if attempt %}
Continuation context:

- This is retry attempt #{{ attempt }} because the ticket is still active.
- Resume from the current workspace instead of restarting.
- Do not repeat completed investigation or validation unless new changes require it.
{% endif %}

Issue context:

- Identifier: {{ issue.identifier }}
- Title: {{ issue.title }}
- State: {{ issue.state }}
- Labels: {{ issue.labels }}
- URL: {{ issue.url }}

Description:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}

Operating rules:

1. This is an unattended session. Do not ask a human to perform optional follow-up work.
2. Work only inside the provided repository checkout.
3. Follow the repository `AGENTS.md` instructions exactly.
4. Use the smallest maintainable solution that satisfies the ticket.
5. Treat missing required auth, missing required tools, and missing required secrets as blockers. Record the blocker in Linear and stop.
6. Never edit `.env.local` or commit secrets.

Required workflow:

1. Determine the issue's current Linear state before doing any code work.
2. Handle states as follows:
   - `Backlog`: do not modify code or the issue; stop.
   - `Todo`: move the issue to `In Progress`, then continue.
   - `In Progress`: execute the task.
   - `In Review`: inspect the existing PR and review feedback.
     - If changes are requested, move the issue back to `In Progress`, implement them, validate, push, and return the issue to `In Review`.
     - If the PR is approved and checks are green, merge it and move the issue to `Done`.
   - `Done`, `Canceled`, `Duplicate`: stop.
3. Maintain exactly one Linear comment with the heading `## Codex Workpad`.
4. Keep the workpad current with:
   - environment stamp,
   - a dependency-aware plan,
   - acceptance criteria,
   - validation checklist,
   - blockers and decisions.
5. Before editing code:
   - inspect `git status`,
   - sync with the latest `origin/main` using the `pull` skill,
   - capture a concrete reproduction signal or current-state observation.
6. Before every push:
   - run scope-appropriate validation,
   - prefer `bun run check` plus targeted tests,
   - run `bun run build` when routing, build output, or app wiring changes.
7. Use the repo-local skills when appropriate:
   - `linear` for raw Linear GraphQL operations,
   - `commit` for commits,
   - `push` for pushing and PR updates; explicitly open and follow `.codex/skills/push/SKILL.md` before creating or updating a PR,
   - `pull` for merging `origin/main`,
   - `land` for final merge shepherding.
8. When a PR exists, treat all actionable top-level comments, inline review comments, and review summaries as blocking until fixed or explicitly answered.
9. Keep the Linear issue linked to the PR.
10. Final message should include only completed actions, validation results, and blockers.
