package store_test

import (
	"context"
	"testing"

	"github.com/expbuild/expbuild/cas/store"
	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestFindMissing(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	ctx := context.Background()
	client.Set(ctx, "cas_test_123", "this is a test", -1)
	store := store.MakeRedisStore()
	digest1 := &pb.Digest{
		Hash:      "test",
		SizeBytes: 123,
	}
	digest2 := &pb.Digest{
		Hash:      "test1",
		SizeBytes: 123,
	}
	missing, err := store.FindMissingBlobs(ctx, []*pb.Digest{digest1, digest2})
	assert.Equal(t, len(missing), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, missing[0].Hash, "test1")
}
