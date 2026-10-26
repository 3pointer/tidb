// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestFTSExplainRedactsCompoundQueryValues(t *testing.T) {
	encode := func(value any) []byte {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		return encoded
	}
	leaf := func(query string, columnID int64) *tipb.Expr {
		return &tipb.Expr{
			Sig: tipb.ScalarFuncSig_FTSMatchWord,
			Val: encode(expression.QueryClause{
				Version:   expression.FTSQueryClauseVersion,
				ScoreMode: expression.FtsScoreModeInherit,
				Query:     query,
				Fields:    []expression.QueryField{{ColID: columnID, IndexField: "STANDARD_V1"}},
			}),
		}
	}
	query := tipb.Expr{
		Sig: tipb.ScalarFuncSig_LogicalAnd,
		Children: []*tipb.Expr{
			leaf("secret-query", 1),
			{
				Sig: tipb.ScalarFuncSig_LogicalOr,
				Val: encode(map[string]any{
					"version":    expression.FTSQueryClauseVersion,
					"score_mode": expression.FtsScoreModeFilter,
				}),
				Children: []*tipb.Expr{
					leaf("/private/path", 2),
					{Sig: tipb.ScalarFuncSig_UnaryNotInt, Children: []*tipb.Expr{leaf("secret-negative", 1)}},
				},
			},
		},
	}
	scan := &PhysicalTableScan{
		DBName: ast.NewCIStr("test"),
		Table: &model.TableInfo{
			Name: ast.NewCIStr("docs"),
			Columns: []*model.ColumnInfo{
				{ID: 1, Name: ast.NewCIStr("title")},
				{ID: 2, Name: ast.NewCIStr("path")},
			},
		},
	}
	scan.SetSchema(expression.NewSchema())
	topK := uint32(10)
	idx := buildFullTextIndexExtra(nil, &tipb.FTSQueryInfo{TopK: &topK, MatchExpr: []tipb.Expr{query}})
	for _, test := range []struct {
		name       string
		normalized bool
		redactMode string
		expected   string
	}{
		{
			name:     "plain",
			expected: "top10 (secret-query IN test.docs.title AND filter((/private/path IN test.docs.path OR NOT(secret-negative IN test.docs.title))))",
		},
		{
			name:       "redacted",
			redactMode: errors.RedactLogEnable,
			expected:   "top10 (? IN test.docs.title AND filter((? IN test.docs.path OR NOT(? IN test.docs.title))))",
		},
		{
			name:       "markers",
			redactMode: errors.RedactLogMarker,
			expected:   "top10 (‹secret-query› IN test.docs.title AND filter((‹/private/path› IN test.docs.path OR NOT(‹secret-negative› IN test.docs.title))))",
		},
		{
			name:       "normalized",
			normalized: true,
			redactMode: errors.RedactLogMarker,
			expected:   "topK (? IN test.docs.title AND filter((? IN test.docs.path OR NOT(? IN test.docs.title))))",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := scan.explainFullTextIndex(idx, test.normalized, test.redactMode, nil)
			require.Equal(t, test.expected, actual)
			if test.normalized || test.redactMode == errors.RedactLogEnable {
				require.NotContains(t, actual, "secret-query")
				require.NotContains(t, actual, "/private/path")
				require.NotContains(t, actual, "secret-negative")
			}
		})
	}
}

func TestFTSExplainLegacyQueryRedaction(t *testing.T) {
	scan := &PhysicalTableScan{}
	scan.SetSchema(expression.NewSchema())
	idx := buildFullTextIndexExtra(nil, &tipb.FTSQueryInfo{
		QueryText:   "legacy-secret",
		ColumnNames: []string{"test.docs.title", "test.docs.body"},
	})
	require.Equal(t, "? IN test.docs.title, test.docs.body", scan.explainFullTextIndex(idx, false, errors.RedactLogEnable, nil))
	require.Equal(t, "? IN test.docs.title, test.docs.body", scan.explainFullTextIndex(idx, true, errors.RedactLogMarker, nil))
	require.Equal(t, "‹legacy-secret› IN test.docs.title, test.docs.body", scan.explainFullTextIndex(idx, false, errors.RedactLogMarker, nil))
}
