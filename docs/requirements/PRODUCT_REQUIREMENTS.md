# agentrail Product Requirements (PRD)

## 1. Objective
Build a Rust-native orchestration control plane that enables chat-driven multi-agent development with strong consistency, high throughput, and operational control.

## 2. Primary user journey
1. User submits requirement in chat app.
2. Orchestrator translates requirement into plan operations.
3. Agents execute tasks in isolated worktrees.
4. Review failures are auto-routed back to the executing agent for repair.
5. Gates rerun automatically and merge happens automatically when green.
6. User receives progress, risks, and verification evidence in chat.
7. User can steer tasks mid-flight (pause/resume/reprioritize).

## 3. Functional requirements
### FR-1 Plan control plane
- Manage plan lifecycle: status, show, claim, complete, mutate, review.
- Preserve DAG dependency semantics and lock transitions.
- Support deterministic checkpoint snapshots for context handoff.

### FR-2 MCP interface
- Expose behavior-compatible toolset for orchestrators.
- Support stdio transport first; HTTP transport as extension.
- Provide machine-readable payloads for critical operations.

### FR-3 Runner control
- Define unified `AgentRunner` contract: start, steer, pause, resume, stop, status, logs.
- Provide `ProcessRunner` baseline and `TmuxRunner` for long-lived interactive sessions.

### FR-4 Human-facing outputs
- Markdown summary output for chat channels.
- Mermaid DAG output for dependency visibility.
- Static HTML dashboard for deep review.

### FR-5 Collaboration and handoff
- Clipboard-style short handoff notes.
- Structured checkpoints for deterministic session restore.

### FR-6 Fully automated delivery loop
- Decompose incoming requirements into DAG tasks with dependency tracking.
- Dispatch tasks to subagents with retry/reassignment policy.
- Auto-run review and validation gates.
- Auto-merge when gate matrix is fully green.
- Auto-clean worktrees and stale runtime state after completion.

## 4. Non-functional requirements
- Throughput: MCP concurrency improves 3-8x vs Python baseline.
- Latency: hot read paths (`status/next/show`) improve by 40-70%.
- Consistency: plan write operations remain CAS-safe with atomic persistence.
- Reliability: recoverable state after orchestrator restart.
- Observability: traceable operation timing and outcomes.
- Automation: no human merge gate in normal operation.

## 5. Constraints
- Worktree isolation solves code-level conflicts but not shared-state correctness.
- Shared orchestration state requires transactional storage.
- Some external agents cannot support true real-time steering.
- Hard failures must escalate with machine-readable diagnostics.

## 6. Acceptance criteria
- Behavior-compatibility level 2 for CLI and MCP contracts.
- Differential test suite against baseline behavior passes.
- Performance and stability targets validated under load.
- Rollback path available during migration.
- End-to-end orchestration dry-run passes (`spawn -> review -> fix -> merge -> cleanup`).
