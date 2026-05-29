# Dirty-Tree Review Progress

## 2026-05-28 Continuation Review

### Scope

- Checked for a newer commit after `a456120`; none was present.
- Found uncommitted changes in `findings.md`, `progress.md`, `task_plan.md`, and all core source files except `Cargo.toml`.
- Reviewed the dirty worktree as the continuation target.
- Rewrote `findings.md`, `task_plan.md`, and `progress.md` with fresh guidance for the current dirty tree.

### Files Read

- `src/main.rs`
- `src/config.rs`
- `src/types.rs`
- `src/bencode.rs`
- `src/routing.rs`
- `src/dht.rs`
- `src/wire.rs`
- `src/metadata.rs`
- `src/store.rs`
- `findings.md`
- `task_plan.md`
- `progress.md`

### Commands Run

- `git status --short`
  - Exit: 0.
  - Summary: dirty worktree with modified planning files and modified source files.
- `git log -3 --oneline --decorate --stat`
  - Exit: 0.
  - Summary: latest commit remains `a456120` on `main` and `codex/post-commit-fixes`; no newer commit exists.
- `git diff --stat`
  - Exit: 0.
  - Summary: dirty diff spans 11 files with 1498 insertions and 417 deletions.
- `git diff -- src/wire.rs`
  - Exit: 0.
  - Summary: reviewed BEP 9 integer conversion changes and new parser tests.
- `git diff -- src/metadata.rs`
  - Exit: 0.
  - Summary: reviewed generic stream abstraction, metadata piece validation, retry handling, stats emission, and tests.
- `git diff -- src/dht.rs`
  - Exit: 0.
  - Summary: reviewed endpoint filtering, HMAC token helpers, announce handling, expected responder tracking, stats counters, and tests.
- `git diff -- src/main.rs`
  - Exit: 0.
  - Summary: reviewed stats event wiring, stats persistence, shutdown helper, and tests.
- `git diff -- src/store.rs`
  - Exit: 0.
  - Summary: reviewed migration check, checked transaction change, and migration/rollback tests.
- `git diff -- src/config.rs src/bencode.rs src/types.rs`
  - Exit: 0.
  - Summary: reviewed config path tests, bencode borrowed lookup changes, and `CrawlStatsEvent`.
- `git diff -- findings.md task_plan.md progress.md`
  - Exit: 0.
  - Summary: confirmed planning files were stale relative to the dirty source changes before rewrite.
- `nl -ba src/dht.rs`
  - Exit: 0.
  - Summary: captured current DHT line numbers.
- `nl -ba src/metadata.rs`
  - Exit: 0.
  - Summary: captured current metadata line numbers.
- `nl -ba src/main.rs`
  - Exit: 0.
  - Summary: captured current CLI/shutdown line numbers.
- `nl -ba src/store.rs`
  - Exit: 0.
  - Summary: captured current store line numbers.
- `nl -ba src/wire.rs`
  - Exit: 0.
  - Summary: captured current wire line numbers.
- `nl -ba src/config.rs`
  - Exit: 0.
  - Summary: captured current config line numbers.
- `nl -ba src/bencode.rs`
  - Exit: 0.
  - Summary: captured current bencode line numbers.
- `nl -ba src/types.rs`
  - Exit: 0.
  - Summary: captured current types line numbers.
- `nl -ba src/routing.rs`
  - Exit: 0.
  - Summary: captured current routing line numbers.
- `cargo check`
  - Exit: 101.
  - Summary: failed on `src/bencode.rs` missing lifetime specifiers at lines 242 and 252, invalid dereference at line 189, and `Vec<u8>` versus `&[u8]` mismatch at line 194.
- `cargo test`
  - Exit: 101.
  - Summary: failed with the same compile errors before running tests.
- `cargo clippy`
  - Exit: 101.
  - Summary: failed with the same compile errors before linting.
- `cargo fmt --check`
  - Exit: 1.
  - Summary: reported formatting diffs in `src/dht.rs` and `src/metadata.rs`.
- `git diff --check`
  - Exit: 0.
  - Summary: no whitespace errors.
- `rg -n "let _ =|unwrap\\(|TODO|todo!|panic!|expect\\(|unchecked_transaction|TcpListener::bind|as u32|as u8|\\.to_vec\\(\\)" src`
  - Exit: 0.
  - Summary: checked remaining unwraps, silent discards, casts, and allocation hot spots after the dirty changes.
- `wc -l findings.md task_plan.md progress.md src/bencode.rs src/dht.rs src/metadata.rs src/main.rs src/store.rs src/wire.rs src/config.rs src/types.rs src/routing.rs`
  - Exit: 0.
  - Summary: confirmed line counts for referenced files.
- `sed -n '1,240p' findings.md`
  - Exit: 0.
  - Summary: re-read the rewritten findings file before finalizing.
- `sed -n '1,240p' task_plan.md`
  - Exit: 0.
  - Summary: re-read the rewritten task plan and confirmed all phases have at most four tasks.
- `sed -n '1,240p' progress.md`
  - Exit: 0.
  - Summary: re-read the rewritten progress log before final consistency edits.
- `perl -Mstrict -Mwarnings -e '...' findings.md task_plan.md progress.md`
  - Exit: 0.
  - Summary: verified every `src/*.rs:N` reference in the planning files points to an existing line in the current worktree.
- `rg -n "^## Phase|^### Task|duplicate|TODO|TBD|stale|a456120" task_plan.md findings.md progress.md`
  - Exit: 0.
  - Summary: confirmed task counts and found no placeholder markers; `a456120` references are intentional review-target context.
- `git status --short`
  - Exit: 0.
  - Summary: final status remains dirty with rewritten planning files plus subordinate source changes.

### Key Review Decisions

- Treated compile failure as the only critical priority; no lower-level behavior can be trusted until the crate builds again.
- Treated DHT response node insertion before expected-responder validation as the main remaining protocol bug.
- Treated metadata stats emission as a correctness issue because the event currently fires before full metadata/file persistence completes.
- Treated high-frequency crawl stat insertion as a performance issue, not a schema issue, because the schema can already represent the counters.
- Did not modify source code during this review; only the three planning/review files were rewritten.
- Final consistency check passed: line references exist, no duplicate tasks were identified, and findings are represented in the fix plan by priority.

## 2026-05-28 Dirty-Tree Plan Execution

### Completed Fixes

- Restored build and formatting after borrowed bencode helper changes.
- Hardened DHT response trust boundaries so unexpected lookup responders cannot add nodes or publish peers.
- Rejected non-public announce sources and expanded IPv4 non-global filtering.
- Changed DHT token buckets to `u64` and hashed the full eight-byte bucket value.
- Made metadata completion report missing rows, insert files before `MetadataFetched`, and skip stats emission on file insert failure.
- Persisted crawl stats only on DHT snapshot events while accumulating metadata successes in memory.
- Exposed latest crawl-history counters in `btfind stats`.
- Tracked metadata workers with `JoinSet` and added shutdown coverage for fetcher-owned workers.
- Logged Ctrl+C listener errors instead of silently discarding them.
- Made old-schema migration tests clean up SQLite sidecar files.
- Removed remaining metadata, wire, and bencode parser lookup allocations covered by the plan.

### Final Verification

- `cargo check`: passed with 0 warnings.
- `cargo test`: passed, 77 passed, 0 failed.
- `cargo clippy`: passed with 0 warnings.
- `cargo fmt --check`: passed.
- `git diff --check`: passed.
