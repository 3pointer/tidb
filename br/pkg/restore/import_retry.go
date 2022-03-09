package restore

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryStatus is the status needed for retrying.
// It likes the `utils.Backoffer`, but more fundamental:
//   this only control the backoff time and knows nothing about what error happens.
type retryStatus struct {
	maxRetry   int
	retryTimes int

	maxBackoff  time.Duration
	nextBackoff time.Duration
}

// Whether in the current state we can retry.
func (rs *retryStatus) ShouldRetry() bool {
	return rs.retryTimes < rs.maxRetry
}

// Get the exponential backoff durion and transform the state.
func (rs *retryStatus) ExponentialBackoff() time.Duration {
	rs.retryTimes++
	backoff := rs.nextBackoff
	rs.nextBackoff *= 2
	if rs.nextBackoff > rs.maxBackoff {
		rs.nextBackoff = rs.maxBackoff
	}
	return backoff
}

func (rs *retryStatus) ConstantBackoff() time.Duration {
	rs.retryTimes++
	return rs.nextBackoff
}

func initialRetryStatus(maxRetryTimes int, initialBackoff, maxBackoff time.Duration) retryStatus {
	return retryStatus{
		maxRetry:    maxRetryTimes,
		maxBackoff:  maxBackoff,
		nextBackoff: initialBackoff,
	}
}

type regionFunc func(ctx context.Context, r *RegionInfo) rpcResult

type overRegionsInRangeController struct {
	start      []byte
	end        []byte
	metaClient SplitClient

	errors error
	rs     retryStatus
}

func overRegionsInRange(start, end []byte, metaClient SplitClient, retryStatus retryStatus) overRegionsInRangeController {
	return overRegionsInRangeController{
		start:      start,
		end:        end,
		metaClient: metaClient,
		rs:         retryStatus,
	}
}

func (o *overRegionsInRangeController) OnError(ctx context.Context, result rpcResult, region *RegionInfo) {
	o.errors = multierr.Append(o.errors, errors.Annotatef(&result, "execute over region %v failed", region.Region))
}

func (o *overRegionsInRangeController) Run(ctx context.Context, f regionFunc) error {
	if !o.rs.ShouldRetry() {
		return o.errors
	}
	tctx, cancel := context.WithTimeout(ctx, importScanRegionTime)
	defer cancel()
	// Scan regions covered by the file range
	regionInfos, errScanRegion := PaginateScanRegion(
		tctx, o.metaClient, o.start, o.end, ScanRegionPaginationLimit)
	if errScanRegion != nil {
		return errors.Trace(errScanRegion)
	}

	// Try to download and ingest the file in every region
	lctx := logutil.ContextWithField(
		ctx,
		logutil.Key("startKey", o.start),
		logutil.Key("endKey", o.end),
	)

	for _, region := range regionInfos {
		result := f(lctx, region)

		if !result.OK() {
			o.OnError(ctx, result, region)
			switch result.StrategyForRetry() {
			case giveUp:
				logutil.CL(ctx).Warn("unexpected error, should stop to retry", logutil.ShortError(&result), logutil.Region(region.Region))
				return o.errors
			case fromThisRegion:
				logutil.CL(ctx).Warn("retry for region", logutil.Region(region.Region), logutil.ShortError(&result))
				time.Sleep(o.rs.ExponentialBackoff())
				continue
			case fromStart:
				logutil.CL(ctx).Warn("retry for execution over regions", logutil.ShortError(&result))
				// TODO: make a backoffer considering more about the error info,
				//       instead of ingore the result and retry.
				time.Sleep(o.rs.ExponentialBackoff())
				return o.Run(ctx, f)
			}
		}
	}
	return nil
}

// rpcResult is the result after executing some RPCs to TiKV.
type rpcResult struct {
	Err        error
	StoreError *errorpb.Error
}

func rpcResultFromError(err error) rpcResult {
	return rpcResult{
		Err: err,
	}
}

func rpcResultOK() rpcResult {
	return rpcResult{}
}

type retryStrategy int

const (
	giveUp retryStrategy = iota
	fromThisRegion
	fromStart
)

func (r *rpcResult) StrategyForRetry() retryStrategy {
	if r.Err != nil {
		return r.StrategyForRetryGoError()
	}
	return r.StrategyForRetryStoreError()
}

func (r *rpcResult) StrategyForRetryStoreError() retryStrategy {
	if r.StoreError == nil {
		return giveUp
	}

	if r.StoreError.GetServerIsBusy() != nil ||
		r.StoreError.GetNotLeader() != nil ||
		r.StoreError.GetRegionNotFound() != nil {
		return fromThisRegion
	}

	return fromStart
}

func (r *rpcResult) StrategyForRetryGoError() retryStrategy {
	if r.Err == nil {
		return giveUp
	}

	if gRPCErr, ok := status.FromError(r.Err); ok {
		switch gRPCErr.Code() {
		case codes.Unavailable, codes.Aborted, codes.ResourceExhausted:
			return fromThisRegion
		}
	}

	return giveUp
}

func (r *rpcResult) Error() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	if r.StoreError != nil {
		return r.StoreError.GetMessage()
	}
	return "<no error>"
}

func (r *rpcResult) OK() bool {
	return r.Err == nil && r.StoreError == nil
}
