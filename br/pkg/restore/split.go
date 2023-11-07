// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retryTimeKey struct{}

var retryTimes = new(retryTimeKey)

type SplitContext struct {
	isRawKv     bool
	needScatter bool
	storeCount  int
	onSplit     OnSplitFunc
}

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client split.SplitClient
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client split.SplitClient) *RegionSplitter {
	return &RegionSplitter{
		client: client,
	}
}

// OnSplitFunc is called before split a range.
type OnSplitFunc func(key [][]byte)

// ExecuteSplit executes regions split and make sure new splitted regions are balance.
// It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) ExecuteSplit(
	ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	storeCount int,
	isRawKv bool,
	onSplit OnSplitFunc,
) error {
	if len(ranges) == 0 {
		log.Info("skip split regions, no range")
		return nil
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("RegionSplitter.Split", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Sort the range for getting the min and max key of the ranges
	sortedRanges, errSplit := SortRanges(ranges, rewriteRules)
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	sortedKeys := make([][]byte, 0, len(sortedRanges))
	for _, r := range sortedRanges {
		sortedKeys = append(sortedKeys, r.EndKey)
	}
	sctx := SplitContext{
		isRawKv:     isRawKv,
		needScatter: true,
		onSplit:     onSplit,
		storeCount:  storeCount,
	}
	return rs.executeSplitByKeys(ctx, sctx, sortedKeys)
}

// executeSplitByKeys will split regions by **sorted** keys with following steps.
// 1. locate regions with correspond keys.
// 2. split these regions with correspond keys.
// 3. make sure new splitted regions are balanced.
func (rs *RegionSplitter) executeSplitByKeys(
	ctx context.Context,
	splitContext SplitContext,
	sortedKeys [][]byte,
) error {
	startTime := time.Now()
	minKey := codec.EncodeBytesExt(nil, sortedKeys[0], splitContext.isRawKv)
	maxKey := codec.EncodeBytesExt(nil, sortedKeys[len(sortedKeys)-1], splitContext.isRawKv)
	scatterRegions := make([]*split.RegionInfo, 0)

	err := utils.WithRetry(ctx, func() error {
		regions, err := split.PaginateScanRegion(ctx, rs.client, minKey, maxKey, split.ScanRegionPaginationLimit)
		if err != nil {
			return err
		}
		splitKeyMap := getSplitKeys(splitContext, sortedKeys, regions)
		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		for regionID, keys := range splitKeyMap {
			log.Info("get split keys for region",
				zap.Int("len", len(keys)),
				zap.Uint64("region", regionID),
				zap.Bool("need scatter", splitContext.needScatter))
			var newRegions []*split.RegionInfo
			region := regionMap[regionID]
			log.Info("split regions", logutil.Region(region.Region), logutil.Keys(keys))
			newRegions, err := rs.splitAndScatterRegions(ctx, splitContext, region, keys)
			if err != nil {
				if strings.Contains(err.Error(), "no valid key") {
					for _, key := range keys {
						// Region start/end keys are encoded. split_region RPC
						// requires raw keys (without encoding).
						log.Error("split regions no valid key",
							logutil.Key("startKey", region.Region.StartKey),
							logutil.Key("endKey", region.Region.EndKey),
							logutil.Key("key", codec.EncodeBytesExt(nil, key, splitContext.isRawKv)))
					}
				}
				return err
			}
			if len(newRegions) != len(keys) {
				log.Warn("split key count and new region count mismatch",
					zap.Int("new region count", len(newRegions)),
					zap.Int("split key count", len(keys)))
			}
			if splitContext.needScatter {
				log.Info("scattered regions", zap.Int("count", len(newRegions)))
				scatterRegions = append(scatterRegions, newRegions...)
			}
			splitContext.onSplit(keys)
		}
		return nil
	}, newSplitBackoffer())
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("finish splitting and scattering regions. and starts to wait", zap.Int("regions", len(scatterRegions)),
		zap.Duration("take", time.Since(startTime)))
	rs.waitRegionsScattered(ctx, scatterRegions, split.ScatterWaitUpperInterval)
	return nil
}

func (rs *RegionSplitter) splitAndScatterRegions(
	ctx context.Context, splitContext SplitContext, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	if len(keys) <= 12 {
		return []*split.RegionInfo{regionInfo}, nil
	}

	// keys are sorted
	storeCount := splitContext.storeCount
	if splitContext.needScatter && len(keys) >= 256 && storeCount > 3 {
		// pick keys by store
		// the max interval should be 100
		interval := mathutil.Max(12, len(keys)*1.0/storeCount)
		pickKeys := make([][]byte, 0, len(keys)/interval)
		i := interval
		for ; i < len(keys); i += interval {
			pickKeys = append(pickKeys, keys[i])
		}
		newPickRegions, err := rs.splitRegionsSync(ctx, regionInfo, pickKeys)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// // scatter these pick keys first
		// // assumption: pick keys would be too much
		// // so we could wait these regions scattered.
		// rs.ScatterRegionsSync(ctx, newPickRegions)
		// return nil, rs.executeSplitByKeys(ctx, splitContext, keys)
		// just scatter these pick regions
		log.Info("new pick region split", zap.Int("newPickRegions", len(newPickRegions)))
		rs.ScatterRegionsSync(ctx, newPickRegions)
		// assume the pick region have scattered and just split remain regions on store.
		splitContext.needScatter = false
		return nil, rs.executeSplitByKeys(ctx, splitContext, keys)
	}
	newRegions, err := rs.splitRegionsSync(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if splitContext.needScatter {
		rs.ScatterRegionsAsync(ctx, newRegions)
	}
	return newRegions, nil
}

// splitRegionsSync perform batchSplit on a region by keys
// and then check the batch split success or not.
func (rs *RegionSplitter) splitRegionsSync(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	if len(keys) == 0 {
		return []*split.RegionInfo{regionInfo}, nil
	}
	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rs.waitRegionsSplitted(ctx, newRegions)
	return newRegions, nil
}

// ScatterRegionsAsync scatter the regions.
// for same reason just log and ignore error.
// See the comments of function waitRegionScattered.
func (rs *RegionSplitter) ScatterRegionsAsync(ctx context.Context, newRegions []*split.RegionInfo) {
	log.Info("start to scatter regions", zap.Int("regions", len(newRegions)))
	// the retry is for the temporary network errors during sending request.
	err := utils.WithRetry(ctx, func() error {
		err := rs.client.ScatterRegions(ctx, newRegions)
		if isUnsupportedError(err) {
			log.Warn("batch scatter isn't supported, rollback to old method", logutil.ShortError(err))
			rs.ScatterRegionsSequentially(
				ctx, newRegions,
				// backoff about 6s, or we give up scattering this region.
				&split.ExponentialBackoffer{
					Attempts:    7,
					BaseBackoff: 100 * time.Millisecond,
				})
			return nil
		}
		return err
	}, &split.ExponentialBackoffer{Attempts: 3, BaseBackoff: 500 * time.Millisecond})
	if err != nil {
		log.Warn("failed to scatter regions", logutil.ShortError(err))
	}
}

// ScatterRegionsSync scatter the regions and wait these region scattered from PD.
// for same reason just log and ignore error.
// See the comments of function waitRegionScattered.
func (rs *RegionSplitter) ScatterRegionsSync(ctx context.Context, newRegions []*split.RegionInfo) {
	rs.ScatterRegionsAsync(ctx, newRegions)
	rs.waitRegionsScattered(ctx, newRegions, split.ScatterWaitUpperInterval)
}

// waitRegionsSplitted check multiple regions have finished the split.
func (rs *RegionSplitter) waitRegionsSplitted(ctx context.Context, splitRegions []*split.RegionInfo) {
	// Wait for a while until the regions successfully split.
	for _, region := range splitRegions {
		rs.waitRegionSplitted(ctx, region.Region.Id)
	}
}

// waitRegionSplitted check single region has finished the split.
func (rs *RegionSplitter) waitRegionSplitted(ctx context.Context, regionID uint64) {
	state := utils.InitialRetryState(
		split.SplitCheckMaxRetryTimes,
		split.SplitCheckInterval,
		split.SplitMaxCheckInterval,
	)
	utils.WithRetry(ctx, func() error { //nolint: errcheck
		ok, err := rs.hasHealthyRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Uint64("regionID", regionID), zap.Error(err))
			return err
		}
		if ok {
			return nil
		}
		return errors.Annotate(berrors.ErrPDSplitFailed, "wait region splitted failed")
	}, &state)
}

// waitRegionsScattered try to wait mutilple regions scatterd in 3 minutes.
// this could timeout, but if many regions scatterd the restore could continue
// so we don't wait long time here.
func (rs *RegionSplitter) waitRegionsScattered(ctx context.Context, scatterRegions []*split.RegionInfo, timeout time.Duration) {
	log.Info("start to wait for scattering regions", zap.Int("regions", len(scatterRegions)))
	startTime := time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		rs.waitRegionScattered(ctx, region)
		if time.Since(startTime) > timeout {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
}

// waitRegionsScattered try to wait single region scatterd
// because we may not get the accurate result of scatter region.
// even we got error here the scatter could also succeed.
// so add a warn log and ignore error does make sense here.
func (rs *RegionSplitter) waitRegionScattered(ctx context.Context, regionInfo *split.RegionInfo) {
	state := utils.InitialRetryState(split.ScatterWaitMaxRetryTimes, split.ScatterWaitInterval, split.ScatterMaxWaitInterval)
	retryCount := 0
	err := utils.WithRetry(ctx, func() error {
		ctx1 := context.WithValue(ctx, retryTimes, retryCount)
		ok, err := rs.isScatterRegionFinished(ctx1, regionInfo.Region.Id)
		if err != nil {
			log.Warn("scatter region failed: do not have the region",
				logutil.Region(regionInfo.Region))
			return err
		}
		if ok {
			return nil
		}
		retryCount++
		return errors.Annotatef(berrors.ErrPDUnknownScatterResult, "try wait region scatter")
	}, &state)
	if err != nil {
		log.Warn("wait scatter region meet error", logutil.Region(regionInfo.Region), logutil.ShortError(err))
	}
}

// ScatterRegionsSequentially scatter the region with some backoffer.
// This function is for testing the retry mechanism.
// For a real cluster, directly use ScatterRegions would be fine.
func (rs *RegionSplitter) ScatterRegionsSequentially(ctx context.Context, newRegions []*split.RegionInfo, backoffer utils.Backoffer) {
	newRegionSet := make(map[uint64]*split.RegionInfo, len(newRegions))
	for _, newRegion := range newRegions {
		newRegionSet[newRegion.Region.Id] = newRegion
	}

	if err := utils.WithRetry(ctx, func() error {
		log.Info("trying to scatter regions...", zap.Int("remain", len(newRegionSet)))
		var errs error
		for _, region := range newRegionSet {
			err := rs.client.ScatterRegion(ctx, region)
			if err == nil {
				// it is safe according to the Go language spec.
				delete(newRegionSet, region.Region.Id)
			} else if !split.PdErrorCanRetry(err) {
				log.Warn("scatter meet error cannot be retried, skipping",
					logutil.ShortError(err),
					logutil.Region(region.Region),
				)
				delete(newRegionSet, region.Region.Id)
			}
			errs = multierr.Append(errs, err)
		}
		return errs
	}, backoffer); err != nil {
		log.Warn("Some regions haven't been scattered because errors.",
			zap.Int("count", len(newRegionSet)),
			// if all region are failed to scatter, the short error might also be verbose...
			logutil.ShortError(err),
			logutil.AbbreviatedArray("failed-regions", newRegionSet, func(i interface{}) []string {
				m := i.(map[uint64]*split.RegionInfo)
				result := make([]string, 0, len(m))
				for id := range m {
					result = append(result, strconv.Itoa(int(id)))
				}
				return result
			}),
		)
	}
}

// hasHealthyRegion is used to check whether region splitted success
func (rs *RegionSplitter) hasHealthyRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := rs.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// the region hasn't get ready.
	if regionInfo == nil {
		return false, nil
	}

	// check whether the region is healthy and report.
	// TODO: the log may be too verbose. we should use Prometheus metrics once it get ready for BR.
	for _, peer := range regionInfo.PendingPeers {
		log.Debug("unhealthy region detected", logutil.Peer(peer), zap.String("type", "pending"))
	}
	for _, peer := range regionInfo.DownPeers {
		log.Debug("unhealthy region detected", logutil.Peer(peer), zap.String("type", "down"))
	}
	// we ignore down peers for they are (normally) hard to be fixed in reasonable time.
	// (or once there is a peer down, we may get stuck at waiting region get ready.)
	return len(regionInfo.PendingPeers) == 0, nil
}

func (rs *RegionSplitter) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, error) {
	resp, err := rs.client.GetOperator(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Annotatef(berrors.ErrPDInvalidResponse, "get operator error: %s", respErr.GetType())
	}
	retryTimes := ctx.Value(retryTimes).(int)
	if retryTimes > 3 {
		log.Info("get operator", zap.Uint64("regionID", regionID), zap.Stringer("resp", resp))
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

// getSplitKeys checks if the regions should be split by the end key of
// the ranges, groups the split keys by region id.
func getSplitKeys(splitContext SplitContext, keys [][]byte, regions []*split.RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	for _, key := range keys {
		if region := NeedSplit(key, regions, splitContext.isRawKv); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			log.Debug("get key for split region",
				logutil.Key("key", key),
				logutil.Key("startKey", region.Region.StartKey),
				logutil.Key("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// NeedSplit checks whether a key is necessary to split, if true returns the split region.
func NeedSplit(splitKey []byte, regions []*split.RegionInfo, isRawKv bool) *split.RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	splitKey = codec.EncodeBytesExt(nil, splitKey, isRawKv)
	for _, region := range regions {
		// If splitKey is the boundary of the region
		if bytes.Equal(splitKey, region.Region.GetStartKey()) {
			return nil
		}
		// If splitKey is in a region
		if region.ContainsInterior(splitKey) {
			return region
		}
	}
	return nil
}

func replacePrefix(s []byte, rewriteRules *RewriteRules) ([]byte, *sst.RewriteRule) {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}

	return s, nil
}

// isUnsupportedError checks whether we should fallback to ScatterRegion API when meeting the error.
func isUnsupportedError(err error) bool {
	s, ok := status.FromError(errors.Cause(err))
	if !ok {
		// Not a gRPC error. Something other went wrong.
		return false
	}
	// In two conditions, we fallback to ScatterRegion:
	// (1) If the RPC endpoint returns UNIMPLEMENTED. (This is just for making test cases not be so magic.)
	// (2) If the Message is "region 0 not found":
	//     In fact, PD reuses the gRPC endpoint `ScatterRegion` for the batch version of scattering.
	//     When the request contains the field `regionIDs`, it would use the batch version,
	//     Otherwise, it uses the old version and scatter the region with `regionID` in the request.
	//     When facing 4.x, BR(which uses v5.x PD clients and call `ScatterRegions`!) would set `regionIDs`
	//     which would be ignored by protocol buffers, and leave the `regionID` be zero.
	//     Then the older version of PD would try to search the region with ID 0.
	//     (Then it consistently fails, and returns "region 0 not found".)
	return s.Code() == codes.Unimplemented ||
		strings.Contains(s.Message(), "region 0 not found")
}

type splitBackoffer struct {
	state utils.RetryState
}

func newSplitBackoffer() *splitBackoffer {
	return &splitBackoffer{
		state: utils.InitialRetryState(split.SplitRetryTimes, split.SplitRetryInterval, split.SplitMaxRetryInterval),
	}
}

func (bo *splitBackoffer) NextBackoff(err error) time.Duration {
	switch {
	case berrors.ErrPDBatchScanRegion.Equal(err):
		log.Warn("inconsistent region info get.", logutil.ShortError(err))
		return time.Second
	case strings.Contains(err.Error(), "no valid key"):
		bo.state.GiveUp()
		return 0
	case berrors.ErrRestoreInvalidRange.Equal(err):
		bo.state.GiveUp()
		return 0
	}
	return bo.state.ExponentialBackoff()
}

func (bo *splitBackoffer) Attempt() int {
	return bo.state.Attempt()
}
