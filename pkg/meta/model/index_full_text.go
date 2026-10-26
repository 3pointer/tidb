// Copyright 2025 PingCAP, Inc.
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

package model

import "github.com/pingcap/tidb/pkg/parser/ast"

// FullTextParserType describes full-text index metadata shared with the SQL parser.
type FullTextParserType = ast.FullTextParserType

// FullTextAnalyzerType describes full-text index metadata shared with the SQL parser.
type FullTextAnalyzerType = ast.FullTextAnalyzerType

// FullTextNgramGranularity describes full-text index metadata shared with the SQL parser.
type FullTextNgramGranularity = ast.FullTextNgramGranularity

// FullTextIndexInfo describes full-text index metadata shared with the SQL parser.
type FullTextIndexInfo = ast.FullTextIndexInfo

// FullTextIndexFieldInfo describes full-text index metadata shared with the SQL parser.
type FullTextIndexFieldInfo = ast.FullTextIndexFieldInfo

// FullTextIndexFieldOption describes full-text index metadata shared with the SQL parser.
type FullTextIndexFieldOption = ast.FullTextIndexFieldOption

// FullTextNgramOption describes full-text index metadata shared with the SQL parser.
type FullTextNgramOption = ast.FullTextNgramOption

// FullTextPathHierarchyOption describes full-text index metadata shared with the SQL parser.
type FullTextPathHierarchyOption = ast.FullTextPathHierarchyOption

const (
	// DefaultFullTextNgramMinGram is the corresponding parser full-text constant.
	DefaultFullTextNgramMinGram = ast.DefaultFullTextNgramMinGram
	// DefaultFullTextNgramMaxGram is the corresponding parser full-text constant.
	DefaultFullTextNgramMaxGram = ast.DefaultFullTextNgramMaxGram
	// DefaultFullTextNgramLowerCase is the corresponding parser full-text constant.
	DefaultFullTextNgramLowerCase = ast.DefaultFullTextNgramLowerCase
	// DefaultFullTextPathHierarchyDelimiter is the corresponding parser full-text constant.
	DefaultFullTextPathHierarchyDelimiter = ast.DefaultFullTextPathHierarchyDelimiter
	// FullTextParserTypeInvalid is the corresponding parser full-text constant.
	FullTextParserTypeInvalid = ast.FullTextParserTypeInvalid
	// FullTextParserTypeStandardV1 is the corresponding parser full-text constant.
	FullTextParserTypeStandardV1 = ast.FullTextParserTypeStandardV1
	// FullTextParserTypeMultilingualV1 is the corresponding parser full-text constant.
	FullTextParserTypeMultilingualV1 = ast.FullTextParserTypeMultilingualV1
	// FullTextParserTypeNgramV1 is the corresponding parser full-text constant.
	FullTextParserTypeNgramV1 = ast.FullTextParserTypeNgramV1
	// FullTextParserTypeExactV1 is the corresponding parser full-text constant.
	FullTextParserTypeExactV1 = ast.FullTextParserTypeExactV1
	// FullTextParserTypePathHierarchyV1 is the corresponding parser full-text constant.
	FullTextParserTypePathHierarchyV1 = ast.FullTextParserTypePathHierarchyV1
	// FullTextNgramGranularityWord is the corresponding parser full-text constant.
	FullTextNgramGranularityWord = ast.FullTextNgramGranularityWord
	// FullTextNgramGranularityChar is the corresponding parser full-text constant.
	FullTextNgramGranularityChar = ast.FullTextNgramGranularityChar
	// DefaultFullTextNgramGranularity is the corresponding parser full-text constant.
	DefaultFullTextNgramGranularity = ast.DefaultFullTextNgramGranularity
)

// GetFullTextNgramGranularityBySQLName returns the NGRAM granularity by a SQL name.
func GetFullTextNgramGranularityBySQLName(name string) (FullTextNgramGranularity, bool) {
	return ast.GetFullTextNgramGranularityBySQLName(name)
}

// GetFullTextParserTypeBySQLName resolves a full-text parser name.
func GetFullTextParserTypeBySQLName(name string) FullTextParserType {
	return ast.GetFullTextParserTypeBySQLName(name)
}

// GetFullTextParserTypeByName resolves a full-text parser name.
func GetFullTextParserTypeByName(name string) FullTextParserType {
	return ast.GetFullTextParserTypeByName(name)
}
