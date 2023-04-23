package ac

import (
	"context"
	"sync"

	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ActionCacheService struct {
	pb.UnimplementedActionCacheServer
	mu sync.RWMutex
	//digest string to actioncache
	results map[string]*pb.ActionResult
}

// NewActionCache returns a new empty ActionCache.
func NewActionCache() *ActionCacheService {
	c := &ActionCacheService{}
	c.Clear()
	return c
}

// Clear removes all results from the cache.
func (c *ActionCacheService) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results = make(map[string]*pb.ActionResult)

}

// Put sets a fake result for a given action digest.
func (c *ActionCacheService) Put(d *pb.Digest, res *pb.ActionResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	digest_key := digest.DigestToString(d)
	c.results[digest_key] = res
}

// Get returns a previously saved fake result for the given action digest.
func (c *ActionCacheService) Get(d *pb.Digest) *pb.ActionResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	digest_key := digest.DigestToString(d)
	res, ok := c.results[digest_key]
	if !ok {
		return nil
	}
	return res
}

// GetActionResult returns a stored result, if it was found.
func (c *ActionCacheService) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (res *pb.ActionResult, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dg := digest.DigestToString(req.ActionDigest)

	if res, ok := c.results[dg]; ok {
		return res, nil
	}
	return nil, status.Error(codes.NotFound, "")
}

// UpdateActionResult sets/updates a given result.
func (c *ActionCacheService) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (res *pb.ActionResult, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dg := digest.DigestToString(req.ActionDigest)

	if req.ActionResult == nil {
		return nil, status.Error(codes.InvalidArgument, "no action result received")
	}
	c.results[dg] = req.ActionResult
	return req.ActionResult, nil
}
