# CSE FTS 与 master 的差异及首轮本地整合

本次从本地 `master` 建立 `codex/fts-cse-integration`，实现提交为 `9ba6ca1166`，目标是把 CSE 的 FTS 增量接入当前架构，提供可直接审阅的代码差异。没有推送远端。执行计划见 [ExecPlan](2026-10-26-fts-cse-integration.md)。

## 比较基线

| 仓库 / 分支 | 固定提交 |
| --- | --- |
| `tidb/master` | `e2f259adf7ddb775cdc02e1514712b534498d05f` |
| `../tidb-cse/main-8.5-keyspace` | `14c98f494b0357fd63e9aaed48749bf640b2f335` |
| 共同祖先 | `999befe4931f29919b9e6b50f419724685e0c832`，2024-11-01 |

从共同祖先算，master 独有 3,848 个提交，CSE 独有 657 个。这个分叉范围远大于 FTS，因此整分支 merge 的 diff 不能用来估算 FTS 移植成本。

本轮主要提取 CSE `7c88221f671f12da1490d461b6431b189ec4e3a1`（#2713），迁入 `83983ee7ffadd65b394c6a81106ef78c6d7e0919`（#2727）的 EXPLAIN 用例组织，并补齐 `cb3eb39a08`（#2738）的 common-handle 完整 point range 检查。#2713 原始 diff 为 67 文件、+19,650/-12,235；去掉生成的 `parser.go` 后为 66 文件、+7,637/-633，其中 source +3,583/-460，tests +3,995/-169，build +59/-4。

## 能力差异

| 能力 | 原 master | CSE 增量 / 本轮整合 |
| --- | --- | --- |
| 基础 FTS | 已有 TiFlash 原生全文索引、`FTS_MATCH_WORD`、score 投影、TopN | 在已有基础上扩展 |
| 索引列 | 一个全文索引一列 | 多列，一个索引可同时覆盖文本和结构化过滤字段 |
| analyzer | 索引级 STANDARD / MULTILINGUAL | 每列多个 analyzer，增加 NGRAM、EXACT、PATH_HIERARCHY |
| analyzer 参数 | 基本 parser 名称 | NGRAM min/max gram、WORD/CHAR、lowercase；路径 delimiter |
| 检索字段 | `FTS_MATCH_WORD(query, column)` | 多字段及 `column WITH PARSER <name>` |
| 查询树 | 一个简单 FTS 条件 | 单个顶层 FTS 条件内部的 AND/OR/NOT 树 |
| 普通过滤 | 通常独立执行 | EXACT `=` / `IN`、路径前缀，及 NGRAM/PATH 的 LIKE/ILIKE 预过滤 |
| 正确性边界 | 基本 FTS 匹配 | 有损预过滤保留 residual；不对有损预过滤做不安全 NOT 改写 |
| 索引选择 | 从表 metadata 找单列全文索引 | 从当前 access paths 绑定，遵守 FORCE/IGNORE INDEX，优选额外过滤覆盖更多的索引 |
| 协议 | 主要 QueryText / Columns / QueryTokenizer | MatchExpr 表达式树，叶子为版本化 JSON QueryClause；过滤节点标记 score_mode |
| MySQL MATCH | 有 MATCH…AGAINST builtin 和 alternative-plan ILIKE 回退 | 保留 master 实现；CSE 增量没有替换这条路径 |

限制仍然存在：独立的多个顶层 FTS conjunct（例如 `WHERE fts_match_word(...) AND fts_match_word(...)`）仍拒绝；scored pure-negative 查询仍拒绝；脏事务和计划缓存限制保留。EXACT 字段服务 `=` / `IN` 过滤，不能直接用作 `FTS_MATCH_WORD` 的文本字段。

## 怎样接入当前 master

1. **语法和 metadata。** CSE 的 `pkg/parser/model` 已在 master 消失，因此把 parser 必须访问的全文类型放到 `pkg/parser/ast/index_full_text.go`，在 `pkg/meta/model/index_full_text.go` 通过 aliases/wrappers 保留服务端接口。保留旧 `parser_type` 读取，增加按 SQL 声明顺序排列的 `index_fields`。该数组顺序同时是引擎 field ID。
2. **DDL 和展示。** 扩展建索引校验、typed analyzer 参数、DDL job 参数及 SHOW CREATE TABLE；沿用 master 的 job 状态机、Starter 限制和索引 readiness 行为。
3. **表达式。** 移植字段级 parser options、QueryClause 编码和 score 语义，同时补齐 clone/hash/equal/memory，以及常量传播、列替换时的 options 保留。
4. **优化器。** 在 DataSource 的 predicate pushdown 边界解析并绑定 FTS，适配 master 可以直接返回 error 的接口；同时收窄 `PossibleAccessPaths` 和 `AllPossibleAccessPaths`，避免统计剪枝恢复旧路径。行范围使用真正的 clustered primary key；普通 FORCE INDEX 扫描也不能把全文字段当作 common handle。完整点范围检查同时使用主键列数。
5. **物理计划。** 继续使用 master 的 `operator/physicalop`、`FTSPushDown{IndexInfo, QueryInfo}` 和 `ColumnarIndexExtra`，把新 MatchExpr 接进去。复合 EXPLAIN 保留 master 的脱敏和 normalized query 行为。

这需要跨模块人工适配，但无需搬 CSE 的整个执行框架或 Go 依赖分叉。`go.mod` / `go.sum` 不需要为 #2713 增量切到 CSE forks。

## 明确保留或未迁移的部分

- 保留 master 的 Starter deployment gate；没有引入 CSE 的 `tidb_enable_fulltext_index` 全局开关，也没有扩大非 Starter 的可用范围。
- 保留 master 已有的 readiness 实现（`049e0e2ba7` 对应能力）；没有重放 CSE 旧 #2252。
- 没有迁移 CSE #2269 的 `UseColumnar` 专用路由。它依赖 CSE 的 columnar/PD 数据面底座，不能作为单一开关接入当前仓库。
- 保留 master 的 MATCH…AGAINST native/ILIKE alternative plan。legacy MATCH 对新增 analyzer 的端到端引擎兼容性仍需另验。
- CSE FULLTEXT grammar 禁止 prefix、表达式、ASC/DESC。这比原 master parser 接受的部分形式更严格，已迁入对应 parser tests；实际 DDL 原先也有相关限制。

## 可以审阅的 SQL 形态

在具备配套 TiFlash/Libclara 的 Starter 环境中，索引可以表达为：

```sql
CREATE TABLE docs (
    id BIGINT PRIMARY KEY,
    title TEXT,
    body TEXT,
    path TEXT,
    status VARCHAR(16),
    FULLTEXT KEY ft (
        title WITH (STANDARD),
        body WITH (NGRAM(min_gram=3, granularity='char')),
        path WITH (PATH_HIERARCHY(delimiter='/')),
        status WITH (EXACT)
    )
);

SELECT id, FTS_MATCH_WORD('database', title) AS score
FROM docs
WHERE FTS_MATCH_WORD('database', title)
  AND status = 'public'
  AND path LIKE '/docs/%'
ORDER BY score DESC
LIMIT 10;
```

这些 SQL 的 parser、DDL 和 planner 行为由本地 mock 测试覆盖；不能把 mock EXPLAIN 通过当作真实引擎检索结果已验证。

## 验证和剩余工作

采用 **Ready** profile：源生成、完整 parser 测试、FTS 定向单测、SQL 集成、Bazel metadata 和 `make lint` 已完成。Classic server 已从本分支构建，既有 MATCH…AGAINST 回退集成用例 **95 项通过**。NextGen + Starter 的 mock SQL、SHOW CREATE、DDL、复合 FTS、score、索引提示、prepared statements 和主键范围检查通过；EXPLAIN 共 **6 个 suite、73 个 SQL case**，录制后以非录制模式再次通过。

本轮实际代码差异（相对固定的 master 基线，另有两份说明文档）：

| 分类 | 文件数 | 新增行 | 删除行 |
| --- | ---: | ---: | ---: |
| 手写 Go + grammar | 41 | 3,637 | 373 |
| 测试及 testdata | 22 | 5,058 | 47 |
| Bazel metadata | 9 | 92 | 3 |
| 生成 Go | 4 | 13,293 | 12,833 |

排除生成 Go 和说明文档，合计 **72 文件、+8,787/-423**。生成文件的重排占原始 diff 的大部分；手写逻辑增量约 3.6K 行。核心入口为 `pkg/parser/ast/index_full_text.go`、`pkg/ddl/index.go`、`pkg/expression/fts_helper.go`、`pkg/planner/core/operator/logicalop/fts_path.go` 和 `pkg/planner/core/operator/physicalop/physical_table_scan.go`。

实际运行命令（仓库根目录；`cd` 已在命令中注明）：

```bash
make parser_ast_visitor
make parser
make -C pkg/parser generate
make parser_fmt
make parser_yacc
make failpoint-enable && (
  trap 'make failpoint-disable' EXIT INT TERM
  gofmt -w pkg/parser/ast/binding__failpoint_binding__.go
  make parser_unit_test
)
(cd pkg/planner/core/operator/logicalop && go run ../../generator/hash64_equals/hash64_equals_generator.go -- hash64_equals_generated.go)
go test ./pkg/expression/fulltext
make bazel_prepare
make lint
```

以下首次组合检查中，expression、model、DDL、hash/equality、physicalop 和既有 core FTS 均返回 `ok`；logicalop 的迁移 fixture 因 master 的 IN 常量去重而失败。改为不同占位常量后，第二条命令通过。

```bash
./tools/check/failpoint-go-test.sh . -tags=intest,deadlock,nextgen -p 2 -run 'FTS|FullText|MatchAgainst|BuildIndexInfoWithTypedExactColumns|CheckIndexInModifiableColumnsUsesFullTextAnalyzer' ./pkg/expression ./pkg/meta/model ./pkg/ddl ./pkg/planner/core/operator/logicalop ./pkg/planner/core/operator/logicalop/logicalop_test ./pkg/planner/core/operator/physicalop ./pkg/planner/core
./tools/check/failpoint-go-test.sh pkg/planner/core/operator/logicalop -tags=intest,deadlock,nextgen -run 'FTS'
./tools/check/failpoint-go-test.sh pkg/planner/core/casetest/fts -tags=intest,deadlock,nextgen -run 'TestTiFlashFTS' -args -record
```

common-handle 的显式范围断言在修复前用下面命令确认失败，错误范围含 path 字符串；修复后录制恢复 `range:[1,1]`，再运行非录制验证。

```bash
./tools/check/failpoint-go-test.sh pkg/planner/core/casetest/fts -tags=intest,deadlock,nextgen -run '^TestTiFlashFTSPathUsesPrimaryIndexForCommonHandleRange$'
make failpoint-enable && (
  trap 'make failpoint-disable' EXIT INT TERM
  (cd pkg/planner/core/casetest/fts && go test -tags=intest,deadlock,nextgen -run '^TestTiFlashFTSPathUsesPrimaryIndexForCommonHandleRange$' -args -record) &&
  go test -tags=intest,deadlock,nextgen -p 2 -run 'FTS|FullText|MatchAgainst|OnlyPointRange' ./pkg/expression/integration_test ./pkg/planner/core ./pkg/planner/core/casetest/fts ./pkg/planner/util
)
GOMAXPROCS=4 make server SERVER_OUT=/tmp/fts-cse-tidb-server
(cd tests/integrationtest && ./run-tests.sh -s /tmp/fts-cse-tidb-server -t planner/core/fulltext_search -d y)
git diff --check
```

最终生成和 lint 均在 failpoint 关闭时执行；临时 binding 文件已清理，引用计数为 0。Bazel 仅保留与本轮源和测试对应的 metadata，`go.mod`、`go.sum`、`DEPS.bzl` 无变化。首次并发 lint 曾读到临时 failpoint 变量，关闭后重跑已通过。

主要风险是引擎协议与滚动升级兼容：配套 TiFlash/Libclara 必须理解新的 `index_fields` 顺序、QueryClause version 1 和 score_mode。DDL job 测试验证旧单 parser 参数的兼容读取，不等于新多 analyzer schema 对旧引擎兼容。未运行真实 TiCI/TiFlash 集群、未比较相关查询的结果集或性能，也未做滚动升级测试。

复核分支：

```bash
git diff --stat master...codex/fts-cse-integration
git diff master...codex/fts-cse-integration -- . ':!pkg/parser/parser.go'
git log --oneline master..codex/fts-cse-integration
```
