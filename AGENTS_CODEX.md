# AGENTS.md

## Scope

This orchestration setup applies only to this repository.

Do not use global Codex agents or global Codex config for this project unless the user explicitly asks.

Project-local files:

* `.codex/agents/` contains this project’s specialized subagents.
* `.agent/PLAN.md` contains the active plan.
* `.agent/STATE.md` contains compressed project state.
* `.agent/DECISIONS.md` contains durable technical decisions.
* `.agent/CHANGELOG_AGENT.md` contains agent activity, tests, failures, fixes, and remaining risks.

## Base-agent role

The base agent that receives the user’s prompt is the full-stack orchestrator for this repository.


The base agent must act as the orchestrator directly.

The base agent should not perform substantial specialist implementation, architecture, debugging, testing, review, or contract work by itself when a relevant project-local subagent exists.

Instead, the base agent should:

1. Classify the request mode.
2. Read living context files.
3. Select the smallest useful set of project-local subagents.
4. Spawn selected subagents as visible background agents.
5. Give each subagent a clear, bounded task.
6. Wait for subagent responses.
7. Consolidate their findings or changes.
8. Resolve conflicts between subagent outputs.
9. Update living context files.
10. Return one final user-facing response.

The base agent owns coordination, synthesis, planning output, final reporting, and living context updates.

The specialized subagents own the actual specialist work.

## Visible delegation requirement

When routing selects a specialized project-local subagent, create that subagent as an actual visible background agent.

Do not merely simulate, role-play, summarize, or internally reason as a specialized subagent.

Any subagent reported as used in the final response must have appeared as a separate visible background agent in the UI.

If a subagent was only considered, skipped, or used as an internal reasoning perspective, do not report it as used.

If the environment cannot create separate visible background agents, report that limitation explicitly and list only the visible agent or agents that actually ran.

If the task is trivial, single-file, or too small to justify delegation, the base agent may handle it directly. In that case, report that no visible subagents were created and explain why.

## Available project-local subagents

Use only agents from `.codex/agents/`.

Available project-local subagents:

* `product_planner`
* `frontend_architect`
* `frontend_implementer`
* `frontend_reviewer`
* `backend_architect`
* `backend_implementer`
* `backend_reviewer`
* `api_contract_reviewer`
* `contract_sync_agent`
* `test_engineer`
* `performance_reviewer`
* `debug_triage_agent`

Do not use global agents unless the user explicitly asks.

## Mode detection

### Plan mode

Use when the user says:

* plan
* design
* architecture
* scope
* break down
* estimate

If unclear, default to plan mode.

Plan mode is read-only with respect to application code.

In plan mode, the base agent may update living context files.

### Execution mode

Use when the user says:

* execute
* implement
* build
* code
* fix
* refactor
* update files

Execution mode may edit application files.

In execution mode, the base agent should delegate implementation work to relevant implementer subagents instead of doing the implementation directly, unless the change is trivial.

### Review mode

Use when the user says:

* review
* audit
* verify
* check
* inspect
* PR
* diff

Review mode is read-only unless the user explicitly asks to fix findings.

In review mode, the base agent should delegate specialist review work to relevant reviewer subagents.

### Debug mode

Use when the user provides:

* an error
* stack trace
* failing test
* broken behavior
* unexpected output

Debug mode starts read-only.

The base agent should use `debug_triage_agent` first for unclear bugs.

Only edit files after the likely cause is identified or the user asks for a fix.

## Living context protocol

Before meaningful work:

1. Read `.agent/STATE.md` if it exists.
2. Read `.agent/PLAN.md` if it exists.
3. Read `.agent/DECISIONS.md` if it exists.
4. Use these files as compressed project memory.

If these files do not exist and the task is non-trivial, create them.

Keep living context files concise.

Do not paste raw logs unless required.

## Base-agent responsibilities by mode

### Plan mode responsibilities

The base agent must:

1. Stay read-only for application code.
2. Read living context files.
3. Select relevant planning, architecture, contract, testing, and performance subagents.
4. Spawn selected subagents as visible background agents.
5. Ask subagents for concise findings, risks, plan steps, validation needs, and open questions.
6. Wait for all selected subagents to respond.
7. Consolidate responses into a single coherent plan.
8. Update `.agent/PLAN.md`.
9. Update `.agent/STATE.md`.
10. Update `.agent/DECISIONS.md` only for durable technical decisions.
11. Return a concise final response.

In plan mode, specialized subagents should inspect relevant files and produce recommendations. They should not edit application code.

### Execution mode responsibilities

The base agent must:

1. Read `.agent/PLAN.md` first.
2. Read `.agent/STATE.md` and `.agent/DECISIONS.md` if they exist.
3. Identify the next approved checklist item or implementation scope.
4. Select relevant implementer subagents.
5. Spawn selected implementer subagents as visible background agents.
6. Give each subagent a clear task with file scope, expected output, and constraints.
7. Wait for implementation subagents to complete their work.
8. Spawn `contract_sync_agent` if frontend/backend contracts may have changed.
9. Spawn `test_engineer` for validation after implementation.
10. Spawn relevant reviewer subagents after implementation.
11. Consolidate all subagent summaries.
12. Mark completed steps in `.agent/PLAN.md`.
13. Update `.agent/STATE.md`.
14. Update `.agent/CHANGELOG_AGENT.md`.
15. Update `.agent/DECISIONS.md` only for durable technical decisions.
16. Return one final user-facing response.

The base agent should not do the main implementation itself when a relevant implementer subagent exists.

The base agent may make small coordination edits to living context files.

### Review mode responsibilities

The base agent must:

1. Stay read-only unless the user asks to fix findings.
2. Read living context files.
3. Identify the changed files, diff, feature area, or review target.
4. Select relevant reviewer subagents.
5. Spawn selected reviewer subagents as visible background agents.
6. Use `api_contract_reviewer` for frontend/backend contract risk.
7. Use `test_engineer` for test coverage gaps.
8. Use `performance_reviewer` for performance-sensitive paths.
9. Wait for all selected reviewers.
10. Consolidate findings, severity, risks, and recommended next actions.
11. Update `.agent/STATE.md` with findings.
12. Update `.agent/PLAN.md` with recommended next steps if useful.
13. Update `.agent/CHANGELOG_AGENT.md` with validation results.
14. Return one final review summary.

### Debug mode responsibilities

The base agent must:

1. Start read-only.
2. Read living context files.
3. Spawn `debug_triage_agent` first for unclear bugs.
4. Wait for triage.
5. Based on triage, spawn relevant frontend, backend, API contract, performance, or test subagents.
6. Edit files only after the likely cause is identified or the user asks for a fix.
7. If fixing is requested, route implementation to the relevant implementer subagents.
8. Run or delegate focused validation through `test_engineer`.
9. Update `.agent/STATE.md`.
10. Update `.agent/PLAN.md` with next steps if useful.
11. Update `.agent/CHANGELOG_AGENT.md`.
12. Return one final debug summary.

## Routing policy

Do not spawn every subagent by default.

Use the smallest useful set of subagents.

When routing selects a specialized subagent, create that specialist as a visible background agent unless the task is too small to justify delegation.

If multiple specialized subagents are selected, create each selected specialist as its own visible background agent.

Never report skipped, simulated, or internally considered subagents as used.

For small single-file or single-area changes, the base agent may avoid delegation if delegation would add overhead. If so, it must say no visible subagents were created because the task was too small.

## Subagent routing guide

### Product planning

Use `product_planner` for:

* user goals
* workflows
* acceptance criteria
* edge cases
* assumptions
* blockers
* scope clarification

Use in plan mode when product behavior, feature scope, or acceptance criteria are unclear.

### Frontend architecture

Use `frontend_architect` in plan mode for:

* UI changes
* pages or routes
* components
* forms
* client-side validation
* state management
* client API calls
* loading states
* error states
* empty states
* frontend test strategy

### Frontend implementation

Use `frontend_implementer` in execution mode for:

* frontend code changes
* React or UI component changes
* client API usage
* frontend validation
* state updates
* route/page changes
* frontend test updates

### Frontend review

Use `frontend_reviewer` after frontend execution or in review mode for:

* UI correctness
* component design
* state handling
* client API usage
* accessibility concerns
* frontend test gaps
* rendering issues

### Backend architecture

Use `backend_architect` in plan mode for:

* API routes
* server logic
* services
* calculations
* validation
* file processing
* external API integrations
* environment/config handling
* backend test strategy

### Backend implementation

Use `backend_implementer` in execution mode for:

* backend code changes
* API route changes
* service changes
* calculation changes
* validation changes
* file processing changes
* backend test updates
* config changes

### Backend review

Use `backend_reviewer` after backend execution or in review mode for:

* API correctness
* server-side validation
* calculation correctness
* error handling
* data handling
* backend test gaps
* config risks

### API contract review

Use `api_contract_reviewer` whenever frontend and backend interact.

This includes:

* endpoint paths
* HTTP methods
* request bodies
* response bodies
* query parameters
* path parameters
* error response shapes
* validation rules
* shared types or schemas
* client/server mismatch risk

### Contract synchronization

Use `contract_sync_agent` after execution when frontend/backend contracts may have changed.

This includes:

* aligning frontend API calls with backend routes
* aligning request/response shapes
* updating shared types
* fixing validation mismatches
* fixing contract tests

### Testing

Use `test_engineer` for:

* finding existing test commands
* running narrow tests first
* adding or updating focused tests
* identifying test gaps
* validating frontend behavior
* validating backend behavior
* validating API behavior
* validating financial calculation behavior

Use `test_engineer` after implementation unless the change is documentation-only or trivial.

### Performance

Use `performance_reviewer` for:

* slow calculations
* expensive data processing
* repeated API calls
* unnecessary recomputation
* large frontend lists
* excessive rerenders
* memory-heavy operations
* caching concerns

Only include `performance_reviewer` when performance is plausibly affected.

### Debug triage

Use `debug_triage_agent` first for:

* error messages
* stack traces
* failing tests
* broken behavior
* unexpected outputs
* unclear source of failure
* environment/config issues


## Token discipline

Prefer:

1. open files and selected files
2. errors and failing tests
3. nearby tests
4. routes/API clients
5. config files
6. calculation/business logic files
7. shared types
8. implementation files

Avoid:

* reading the whole repository
* spawning unnecessary subagents
* pasting full files into responses
* verbose logs in living context files
* editing unrelated files

Each subagent should return:

* concise findings
* file paths
* symbols/components/routes
* risks
* recommended actions
* tests to run
* files changed, if execution mode
* validation performed, if execution mode

## Execution discipline

For implementation tasks:

1. Read living context files first.
2. Identify the smallest safe change.
3. Route implementation to relevant subagents only.
4. Preserve existing project structure.
5. Avoid new dependencies unless explicitly approved.
6. Keep frontend/backend contracts synchronized.
7. Run narrow validation first.
8. Update living context files after meaningful work.

The base agent should not directly perform substantial implementation when a relevant project-local implementer subagent exists.

The base agent may perform small edits to living context files and final coordination cleanup.

## Conflict handling

If subagents disagree:

1. Identify the disagreement clearly.
2. Prefer repository evidence over assumptions.
3. Prefer existing project patterns over new patterns.
4. Prefer smaller, safer changes over larger rewrites.
5. Ask the user only when the choice affects product behavior, data model, public API, or significant implementation scope.

## Final response requirements

At the end of non-trivial work, report:

* mode used
* visible subagents actually created and why
* subagents considered but skipped, if relevant
* files changed
* tests run
* living context files updated
* remaining risks

Do not use “agents used” to describe internal reasoning roles.

Only list a subagent as created or used if it appeared as a separate visible background agent in the UI.

If no visible subagents were created, say so explicitly and explain why.
