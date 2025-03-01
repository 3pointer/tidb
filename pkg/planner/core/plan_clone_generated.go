// Copyright 2024 PingCAP, Inc.
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

// Code generated by plan_clone_generator; DO NOT EDIT IT DIRECTLY.

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
)

func clonePhysicalPlansForPlanCache(newCtx base.PlanContext, plans []base.PhysicalPlan) ([]base.PhysicalPlan, bool) {
	clonedPlans := make([]base.PhysicalPlan, len(plans))
	for i, plan := range plans {
		cloned, ok := plan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPlans[i] = cloned.(base.PhysicalPlan)
	}
	return clonedPlans, true
}

func cloneExpressionsForPlanCache(exprs []expression.Expression) []expression.Expression {
	if exprs == nil {
		return nil
	}
	allSafe := true
	for _, e := range exprs {
		if !e.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return exprs
	}
	cloned := make([]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		if e.SafeToShareAcrossSession() {
			cloned = append(cloned, e)
		} else {
			cloned = append(cloned, e.Clone())
		}
	}
	return cloned
}

func cloneExpression2DForPlanCache(exprs [][]expression.Expression) [][]expression.Expression {
	if exprs == nil {
		return nil
	}
	cloned := make([][]expression.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, cloneExpressionsForPlanCache(e))
	}
	return cloned
}

func cloneScalarFunctionsForPlanCache(scalarFuncs []*expression.ScalarFunction) []*expression.ScalarFunction {
	if scalarFuncs == nil {
		return nil
	}
	allSafe := true
	for _, f := range scalarFuncs {
		if !f.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return scalarFuncs
	}
	cloned := make([]*expression.ScalarFunction, 0, len(scalarFuncs))
	for _, f := range scalarFuncs {
		if f.SafeToShareAcrossSession() {
			cloned = append(cloned, f)
		} else {
			cloned = append(cloned, f.Clone().(*expression.ScalarFunction))
		}
	}
	return cloned
}

func cloneColumnsForPlanCache(cols []*expression.Column) []*expression.Column {
	if cols == nil {
		return nil
	}
	allSafe := true
	for _, c := range cols {
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return cols
	}
	cloned := make([]*expression.Column, 0, len(cols))
	for _, c := range cols {
		if c == nil {
			cloned = append(cloned, nil)
			continue
		}
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Column))
		}
	}
	return cloned
}

func cloneConstantsForPlanCache(constants []*expression.Constant) []*expression.Constant {
	if constants == nil {
		return nil
	}
	allSafe := true
	for _, c := range constants {
		if !c.SafeToShareAcrossSession() {
			allSafe = false
			break
		}
	}
	if allSafe {
		return constants
	}
	cloned := make([]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		if c.SafeToShareAcrossSession() {
			cloned = append(cloned, c)
		} else {
			cloned = append(cloned, c.Clone().(*expression.Constant))
		}
	}
	return cloned
}

func cloneConstant2DForPlanCache(constants [][]*expression.Constant) [][]*expression.Constant {
	if constants == nil {
		return nil
	}
	cloned := make([][]*expression.Constant, 0, len(constants))
	for _, c := range constants {
		cloned = append(cloned, cloneConstantsForPlanCache(c))
	}
	return cloned
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalTableScan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalTableScan)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	cloned.AccessCondition = cloneExpressionsForPlanCache(op.AccessCondition)
	cloned.filterCondition = cloneExpressionsForPlanCache(op.filterCondition)
	cloned.LateMaterializationFilterCondition = cloneExpressionsForPlanCache(op.LateMaterializationFilterCondition)
	cloned.Ranges = util.CloneRanges(op.Ranges)
	cloned.HandleIdx = make([]int, len(op.HandleIdx))
	copy(cloned.HandleIdx, op.HandleIdx)
	if op.HandleCols != nil {
		cloned.HandleCols = op.HandleCols.Clone(newCtx.GetSessionVars().StmtCtx)
	}
	cloned.ByItems = util.CloneByItemss(op.ByItems)
	cloned.PlanPartInfo = op.PlanPartInfo.Clone()
	if op.SampleInfo != nil {
		return nil, false
	}
	cloned.constColsByCond = make([]bool, len(op.constColsByCond))
	copy(cloned.constColsByCond, op.constColsByCond)
	if op.runtimeFilterList != nil {
		return nil, false
	}
	if op.AnnIndexExtra != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexScan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexScan)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	cloned.AccessCondition = cloneExpressionsForPlanCache(op.AccessCondition)
	cloned.IdxCols = cloneColumnsForPlanCache(op.IdxCols)
	cloned.IdxColLens = make([]int, len(op.IdxColLens))
	copy(cloned.IdxColLens, op.IdxColLens)
	cloned.Ranges = util.CloneRanges(op.Ranges)
	if op.GenExprs != nil {
		return nil, false
	}
	cloned.ByItems = util.CloneByItemss(op.ByItems)
	if op.pkIsHandleCol != nil {
		cloned.pkIsHandleCol = op.pkIsHandleCol.Clone().(*expression.Column)
	}
	cloned.constColsByCond = make([]bool, len(op.constColsByCond))
	copy(cloned.constColsByCond, op.constColsByCond)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalSelection) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalSelection)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.Conditions = cloneExpressionsForPlanCache(op.Conditions)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalProjection) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalProjection)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	cloned.Exprs = cloneExpressionsForPlanCache(op.Exprs)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalSort) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalSort)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.ByItems = util.CloneByItemss(op.ByItems)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalTopN) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalTopN)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.ByItems = util.CloneByItemss(op.ByItems)
	cloned.PartitionBy = util.CloneSortItems(op.PartitionBy)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalStreamAgg) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalStreamAgg)
	*cloned = *op
	basePlan, baseOK := op.basePhysicalAgg.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.basePhysicalAgg = *basePlan
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalHashAgg) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalHashAgg)
	*cloned = *op
	basePlan, baseOK := op.basePhysicalAgg.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.basePhysicalAgg = *basePlan
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalHashJoin) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalHashJoin)
	*cloned = *op
	basePlan, baseOK := op.basePhysicalJoin.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.basePhysicalJoin = *basePlan
	cloned.EqualConditions = cloneScalarFunctionsForPlanCache(op.EqualConditions)
	cloned.NAEqualConditions = cloneScalarFunctionsForPlanCache(op.NAEqualConditions)
	if op.runtimeFilterList != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalMergeJoin) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalMergeJoin)
	*cloned = *op
	basePlan, baseOK := op.basePhysicalJoin.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.basePhysicalJoin = *basePlan
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalTableReader) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalTableReader)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	if op.tablePlan != nil {
		tablePlan, ok := op.tablePlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.tablePlan = tablePlan.(base.PhysicalPlan)
	}
	cloned.TablePlans = flattenPushDownPlan(cloned.tablePlan)
	cloned.PlanPartInfo = op.PlanPartInfo.Clone()
	if op.TableScanAndPartitionInfos != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexReader) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexReader)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	if op.indexPlan != nil {
		indexPlan, ok := op.indexPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.indexPlan = indexPlan.(base.PhysicalPlan)
	}
	cloned.IndexPlans = flattenPushDownPlan(cloned.indexPlan)
	cloned.OutputColumns = cloneColumnsForPlanCache(op.OutputColumns)
	cloned.PlanPartInfo = op.PlanPartInfo.Clone()
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PointGetPlan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PointGetPlan)
	*cloned = *op
	cloned.Plan = *op.Plan.CloneWithNewCtx(newCtx)
	probeParents, ok := clonePhysicalPlansForPlanCache(newCtx, op.probeParents)
	if !ok {
		return nil, false
	}
	cloned.probeParents = probeParents
	cloned.PartitionNames = util.CloneCIStrs(op.PartitionNames)
	cloned.schema = op.schema.Clone()
	if op.PartitionIdx != nil {
		cloned.PartitionIdx = new(int)
		*cloned.PartitionIdx = *op.PartitionIdx
	}
	if op.Handle != nil {
		cloned.Handle = op.Handle.Copy()
	}
	if op.HandleConstant != nil {
		cloned.HandleConstant = op.HandleConstant.Clone().(*expression.Constant)
	}
	cloned.IndexValues = util.CloneDatums(op.IndexValues)
	cloned.IndexConstants = cloneConstantsForPlanCache(op.IndexConstants)
	cloned.IdxCols = cloneColumnsForPlanCache(op.IdxCols)
	cloned.IdxColLens = make([]int, len(op.IdxColLens))
	copy(cloned.IdxColLens, op.IdxColLens)
	cloned.AccessConditions = cloneExpressionsForPlanCache(op.AccessConditions)
	cloned.ctx = newCtx
	cloned.accessCols = cloneColumnsForPlanCache(op.accessCols)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *BatchPointGetPlan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(BatchPointGetPlan)
	*cloned = *op
	cloned.baseSchemaProducer = *op.baseSchemaProducer.CloneWithNewCtx(newCtx)
	probeParents, ok := clonePhysicalPlansForPlanCache(newCtx, op.probeParents)
	if !ok {
		return nil, false
	}
	cloned.probeParents = probeParents
	cloned.PartitionNames = util.CloneCIStrs(op.PartitionNames)
	cloned.ctx = newCtx
	cloned.Handles = util.CloneHandles(op.Handles)
	cloned.HandleParams = cloneConstantsForPlanCache(op.HandleParams)
	cloned.IndexValues = util.CloneDatum2D(op.IndexValues)
	cloned.IndexValueParams = cloneConstant2DForPlanCache(op.IndexValueParams)
	cloned.AccessConditions = cloneExpressionsForPlanCache(op.AccessConditions)
	cloned.IdxCols = cloneColumnsForPlanCache(op.IdxCols)
	cloned.IdxColLens = make([]int, len(op.IdxColLens))
	copy(cloned.IdxColLens, op.IdxColLens)
	cloned.PartitionIdxs = make([]int, len(op.PartitionIdxs))
	copy(cloned.PartitionIdxs, op.PartitionIdxs)
	cloned.accessCols = cloneColumnsForPlanCache(op.accessCols)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalLimit) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalLimit)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	cloned.PartitionBy = util.CloneSortItems(op.PartitionBy)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexJoin) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexJoin)
	*cloned = *op
	basePlan, baseOK := op.basePhysicalJoin.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.basePhysicalJoin = *basePlan
	if op.innerPlan != nil {
		innerPlan, ok := op.innerPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.innerPlan = innerPlan.(base.PhysicalPlan)
	}
	cloned.Ranges = op.Ranges.CloneForPlanCache()
	cloned.KeyOff2IdxOff = make([]int, len(op.KeyOff2IdxOff))
	copy(cloned.KeyOff2IdxOff, op.KeyOff2IdxOff)
	cloned.IdxColLens = make([]int, len(op.IdxColLens))
	copy(cloned.IdxColLens, op.IdxColLens)
	cloned.CompareFilters = op.CompareFilters.Copy()
	cloned.OuterHashKeys = cloneColumnsForPlanCache(op.OuterHashKeys)
	cloned.InnerHashKeys = cloneColumnsForPlanCache(op.InnerHashKeys)
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexHashJoin) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexHashJoin)
	*cloned = *op
	inlj, ok := op.PhysicalIndexJoin.CloneForPlanCache(newCtx)
	if !ok {
		return nil, false
	}
	cloned.PhysicalIndexJoin = *inlj.(*PhysicalIndexJoin)
	cloned.Self = cloned
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexLookUpReader) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexLookUpReader)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	if op.indexPlan != nil {
		indexPlan, ok := op.indexPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.indexPlan = indexPlan.(base.PhysicalPlan)
	}
	if op.tablePlan != nil {
		tablePlan, ok := op.tablePlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.tablePlan = tablePlan.(base.PhysicalPlan)
	}
	cloned.IndexPlans = flattenPushDownPlan(cloned.indexPlan)
	cloned.TablePlans = flattenPushDownPlan(cloned.tablePlan)
	if op.ExtraHandleCol != nil {
		cloned.ExtraHandleCol = op.ExtraHandleCol.Clone().(*expression.Column)
	}
	cloned.PushedLimit = op.PushedLimit.Clone()
	cloned.CommonHandleCols = cloneColumnsForPlanCache(op.CommonHandleCols)
	cloned.PlanPartInfo = op.PlanPartInfo.Clone()
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalIndexMergeReader) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexMergeReader)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	cloned.PushedLimit = op.PushedLimit.Clone()
	cloned.ByItems = util.CloneByItemss(op.ByItems)
	partialPlans, ok := clonePhysicalPlansForPlanCache(newCtx, op.partialPlans)
	if !ok {
		return nil, false
	}
	cloned.partialPlans = partialPlans
	if op.tablePlan != nil {
		tablePlan, ok := op.tablePlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.tablePlan = tablePlan.(base.PhysicalPlan)
	}
	cloned.PartialPlans = make([][]base.PhysicalPlan, len(op.PartialPlans))
	for i, plan := range cloned.partialPlans {
		cloned.PartialPlans[i] = flattenPushDownPlan(plan)
	}
	cloned.TablePlans = flattenPushDownPlan(cloned.tablePlan)
	cloned.PlanPartInfo = op.PlanPartInfo.Clone()
	if op.HandleCols != nil {
		cloned.HandleCols = op.HandleCols.Clone(newCtx.GetSessionVars().StmtCtx)
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *Update) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(Update)
	*cloned = *op
	cloned.baseSchemaProducer = *op.baseSchemaProducer.CloneWithNewCtx(newCtx)
	cloned.OrderedList = util.CloneAssignments(op.OrderedList)
	if op.SelectPlan != nil {
		SelectPlan, ok := op.SelectPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.SelectPlan = SelectPlan.(base.PhysicalPlan)
	}
	if op.PartitionedTable != nil {
		return nil, false
	}
	if op.FKChecks != nil {
		return nil, false
	}
	if op.FKCascades != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *Delete) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(Delete)
	*cloned = *op
	cloned.baseSchemaProducer = *op.baseSchemaProducer.CloneWithNewCtx(newCtx)
	if op.SelectPlan != nil {
		SelectPlan, ok := op.SelectPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.SelectPlan = SelectPlan.(base.PhysicalPlan)
	}
	if op.FKChecks != nil {
		return nil, false
	}
	if op.FKCascades != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *Insert) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(Insert)
	*cloned = *op
	cloned.baseSchemaProducer = *op.baseSchemaProducer.CloneWithNewCtx(newCtx)
	cloned.Lists = cloneExpression2DForPlanCache(op.Lists)
	cloned.OnDuplicate = util.CloneAssignments(op.OnDuplicate)
	cloned.GenCols = op.GenCols.Copy()
	if op.SelectPlan != nil {
		SelectPlan, ok := op.SelectPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.SelectPlan = SelectPlan.(base.PhysicalPlan)
	}
	if op.FKChecks != nil {
		return nil, false
	}
	if op.FKCascades != nil {
		return nil, false
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalLock) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalLock)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	if op.TblID2Handle != nil {
		cloned.TblID2Handle = make(map[int64][]util.HandleCols, len(op.TblID2Handle))
		for k, v := range op.TblID2Handle {
			cloned.TblID2Handle[k] = util.CloneHandleCols(newCtx.GetSessionVars().StmtCtx, v)
		}
	}
	if op.TblID2PhysTblIDCol != nil {
		cloned.TblID2PhysTblIDCol = make(map[int64]*expression.Column, len(op.TblID2PhysTblIDCol))
		for k, v := range op.TblID2PhysTblIDCol {
			cloned.TblID2PhysTblIDCol[k] = v.Clone().(*expression.Column)
		}
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalUnionScan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalUnionScan)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.Conditions = cloneExpressionsForPlanCache(op.Conditions)
	if op.HandleCols != nil {
		cloned.HandleCols = op.HandleCols.Clone(newCtx.GetSessionVars().StmtCtx)
	}
	return cloned, true
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalUnionAll) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalUnionAll)
	*cloned = *op
	basePlan, baseOK := op.physicalSchemaProducer.cloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.physicalSchemaProducer = *basePlan
	return cloned, true
}
