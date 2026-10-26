# Integrate CSE full-text search into master

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current according to repository-root PLANS.md.

## Purpose / Big Picture


Create a local branch that makes the FTS (full-text search) difference between TiDB master and the adjacent tidb-cse repository reviewable. Extend master's existing Starter full-text indexes with multi-column indexes, multiple analyzers per column, structured compound queries, and safe equality/pattern prefilters. An analyzer converts source text to indexed tokens. SQL correctness still requires residual filters where the index can only generate candidates.

This is an integration experiment, not a claim that an arbitrary TiFlash binary supports the new index metadata and query payload. The final report must distinguish source integration, local checks, and end-to-end engine verification.

## Progress


- [x] (2026-10-26) Pin master e2f259adf7ddb775cdc02e1514712b534498d05f and CSE main-8.5-keyspace 14c98f494b0357fd63e9aaed48749bf640b2f335.
- [x] Identify existing master native FTS, MATCH/AGAINST fallback, index readiness, and CSE incremental commit 7c88221f671f12da1490d461b6431b189ec4e3a1.
- [x] Import local source objects and create codex/fts-cse-integration from master.
- [x] (2026-10-26) Port parser and metadata, preserving master's package layout.
- [x] (2026-10-26) Port DDL metadata validation, lifecycle arguments, and SHOW output.
- [x] (2026-10-26) Port expression semantics and planner access paths, preserving master's fallback and physical operator layout.
- [x] (2026-10-26) Regenerate parser and Bazel metadata, run scoped tests and Ready checks; Classic server build and 95 fallback SQL integration cases pass.
- [x] (2026-10-26) Self-review and commit the integration as 9ba6ca1166; complete the reproducible comparison report.

## Surprises & Discoveries


Master already supports native FTS_MATCH_WORD, STANDARD/MULTILINGUAL analyzers, score projection and TopN. It additionally has MATCH/AGAINST-to-ILIKE fallback. The source delta is concentrated in CSE #2713 (67 files including a large generated parser diff), with #2727 reorganizing EXPLAIN golden tests. The source's parser/model package no longer exists in master; metadata lives under pkg/meta/model. Physical scans also moved into pkg/planner/core/operator/physicalop.

The repositories share ancestor 999befe4931f29919b9e6b50f419724685e0c832, with 3848 master-only and 657 CSE-only commits. A whole-branch merge would mix unrelated architecture changes. The local fetch succeeded; automatic background Git maintenance reported pre-existing reflog lock files. Branch creation succeeded, and those unrelated locks were left untouched.

Master's generated AST visitor requires explicit node inventory entries and direct child fields. FTSQueryFieldExpr therefore contains a ColumnNameExpr child, rather than constructing a temporary one during Accept. Both added AST nodes and generated visitor inventories are covered by the complete parser tests.

Master's statistics collection can restore PossibleAccessPaths from AllPossibleAccessPaths. The port must narrow both immediately after consuming the FTS predicate; otherwise an unrelated ordinary index can replace the bound FTS path. A focused score/TopN case with one useful and one unused ordinary index exercises this interaction. Handle ranges are derived on a scratch ordinary TiFlash table path, preserving the common primary handle independently of the selected full-text index.

Golden review also caught the non-FTS-function path: FORCE INDEX on a multi-column full-text index must still derive clustered row ranges from the actual primary index. The first recording incorrectly placed a path string into a BIGINT primary-key range. An explicit assertion failed before the repair (see /tmp/fts-cse-common-handle-before.log). Port the missing primary-index derivation from #2713 and the complete-point check from CSE cb3eb39a08 (#2738). Master's added handle-estimation logic requires passing the actual range index, including its column count, rather than only its ID.

The initial Bazel run spent roughly eleven minutes downloading its own Go 1.25.14 SDK through the local proxy, then resumed normally. Parser generation, complete parser tests, the hash generator, and the standalone LIKE helper tests have passed. The first make lint overlapped failpoint instrumentation and reported its temporary _err_ variables; it must be rerun after failpoint cleanup. Final Bazel generation must also run after cleanup to exclude temporary binding files.

## Decision Log


- Decision: Port the FTS increment from CSE #2713 rather than merge all CSE commits.
  Rationale: Master already contains equivalent baseline functionality and newer APIs.
  Date/Author: 2026-10-26 / Codex.
- Decision: Keep master's deployment-mode gate, MATCH fallback, readiness behavior, dependencies, and physical operator layout.
  Rationale: These are independent contracts. CSE's UseColumnar routing depends on its separate data-plane architecture.
  Date/Author: 2026-10-26 / Codex.
- Decision: Preserve all pre-existing untracked user files and limit commits to explicit task paths.
  Rationale: The shared checkout contains unrelated drafts and tools.
  Date/Author: 2026-10-26 / Codex.
- Decision: Include the related CSE #2738 complete-point check and an explicit primary-range regression assertion.
  Rationale: A generated golden file alone can accept a wrong range. Both explicit FTS and ordinary forced columnar scans must use actual primary-key encoding.
  Date/Author: 2026-10-26 / Codex.

## Outcomes & Retrospective


Code integration and local verification are complete. Excluding generated Go and the two documents, the branch contains 72 files with +8,787/-423 lines; handwritten Go and grammar account for +3,637/-373. The four generated Go files contribute +13,293/-12,833. The comparison report records exact commands, 73 EXPLAIN cases, the failing-before/passing-after primary-range assertion, and 95 Classic fallback SQL cases. New analyzer execution and performance still require a compatible TiFlash/Libclara deployment; no production-readiness or rolling-upgrade claim is made.

## Context and Orientation


pkg/parser/parser.y and pkg/parser/ast define SQL syntax. pkg/meta/model defines durable index metadata. pkg/ddl validates and persists index jobs. pkg/expression represents FTS predicates, including options and structured payloads. pkg/planner/core/operator/logicalop chooses index access paths. pkg/planner/core/fts_resolve_index.go resolves score/TopN projections. pkg/planner/core/operator/physicalop/physical_table_scan.go emits scan information for TiFlash. CSE's QueryClause JSON version 1 is carried inside an existing protobuf expression value; engine support must be verified separately.

## Plan of Work


Apply source changes by subsystem, adapting only the FTS deltas to current APIs. Keep parser-independent metadata shared through parser/ast if grammar needs it and use aliases in meta/model where necessary to avoid import cycles. Retain legacy parser_type metadata compatibility while adding index_fields. Regenerate generated outputs from source. Port focused tests for syntax, cloning/equality, DDL validation, access-path selection, residual filters, and query payload structure. Keep existing master FTS fallback tests.

## Concrete Steps


Run from /Users/lc/lc/code/tidb. Inspect source with git show 7c88221f67 and git diff 7c88221f67^ 7c88221f67 -- <owned paths>. Use adapted patches rather than entire old shared files. After all edits run make parser, make parser_yacc, make parser_fmt, make bazel_prepare before package builds/tests. Run make parser_unit_test and scoped package commands through tools/check/failpoint-go-test.sh when failpoints are present. Run make lint for Ready delivery. Keep exact command evidence in this document or its linked report.

## Validation and Acceptance


SQL parsing and restore must accept multi-column/per-column analyzer syntax and FTS field WITH(parser). DDL must preserve field declaration order, reject invalid analyzer options, and maintain legacy metadata. Planner tests must demonstrate AND/OR query payloads, required residual LIKE/ILIKE filters, safe hints/access paths, score projection and TopN, and unchanged legacy MATCH fallback. Local tests cannot establish execution correctness without compatible TiFlash/Libclara; record this explicitly. Test failures require diagnosis and a clear final status.

## Idempotence and Recovery


The source ref is refs/remotes/cse-local/main-8.5-keyspace. Reapplying patches requires checking git diff first. Do not reset or clean user files. The experiment is isolated on codex/fts-cse-integration; switching back to master after committing recovers the original tracked tree. Do not publish or push without a request.

## Artifacts and Notes


Use git diff --stat master...codex/fts-cse-integration and git diff --numstat with generated parser output separated. The comparison report must name both pinned SHAs and explain omitted CSE routing changes.

Implementation commit: 9ba6ca1166. The comparison and validation report is docs/design/2026-10-26-fts-cse-comparison.md. All temporary failpoint files were removed, the failpoint reference count is zero, and the standalone SQL integration runner exited successfully after cleaning up its local server.

## Interfaces and Dependencies


Preserve existing tipb.FTSQueryInfo and ScalarFuncSig_FTSMatchWord interfaces. Introduce FullTextIndexFieldInfo and typed analyzer options compatible with CSE's JSON metadata. FTSMatchWordOptions must clone/hash/compare consistently with expression semantics. Logical FTSPushDown must carry compound query structure without requiring a CSE-only Go module fork. No unrelated CSE client-go, PD or kvproto replacements belong in this branch.
