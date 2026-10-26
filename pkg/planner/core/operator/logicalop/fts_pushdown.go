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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

var _ base.HashEquals = &FTSPushDown{}

// Hash64 includes the selected index and the complete query, including parser
// choices and score modes encoded in each match expression's metadata.
func (fts *FTSPushDown) Hash64(h base.Hasher) {
	if fts.IndexInfo == nil {
		h.HashByte(base.NilFlag)
	} else {
		h.HashByte(base.NotNilFlag)
		fts.IndexInfo.Hash64(h)
	}
	if fts.QueryInfo == nil {
		h.HashByte(base.NilFlag)
		return
	}
	h.HashByte(base.NotNilFlag)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(protoadapt.MessageV2Of(fts.QueryInfo))
	if err != nil {
		h.HashString(fts.QueryInfo.String())
		return
	}
	h.HashBytes(encoded)
}

// Equals compares the selected index and full pushdown query.
func (fts *FTSPushDown) Equals(other any) bool {
	fts2, ok := other.(*FTSPushDown)
	if !ok {
		return false
	}
	if fts == nil {
		return fts2 == nil
	}
	if fts2 == nil || !fts.IndexInfo.Equals(fts2.IndexInfo) {
		return false
	}
	return proto.Equal(protoadapt.MessageV2Of(fts.QueryInfo), protoadapt.MessageV2Of(fts2.QueryInfo))
}
