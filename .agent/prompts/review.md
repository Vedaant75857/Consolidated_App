

Review:
{{TASK_OR_DIFF}}

Instructions:
- Read the living context files first.
- Stay read-only unless explicitly asked to fix findings.
- Route to the smallest useful set of project-local review agents.
- Check frontend/backend contract alignment if relevant.
- Check performance risk if calculations, large data, repeated API calls, or heavy rendering are involved.
- Update `.agent/STATE.md`, `.agent/PLAN.md`, and `.agent/CHANGELOG_AGENT.md` with findings.
- Return one consolidated review.
- Report agents used and why.