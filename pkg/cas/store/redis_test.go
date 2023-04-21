package store_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/expbuild/expbuild/pkg/cas/store"
	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {

	code := m.Run()
	os.Exit(code)
}

func TestFindMissing(t *testing.T) {

	mr, err := miniredis.Run()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mr.Close()
	mr.Set("cas_test_123", "this is a test")

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	ctx := context.Background()
	//client.Set(ctx, "cas_test_123", "this is a test", -1)
	store := store.MakeRedisStoreWithClient(client)
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

func TestSetGetBlob(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mr.Close()
	mr.Set("cas_test_123", "this is a test")

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	store := store.MakeRedisStoreWithClient(client)

	digest1 := &pb.Digest{
		Hash:      "test",
		SizeBytes: 123,
	}
	store.PutBlob(ctx, digest1, []byte("this is a test"))
	data, err := store.GetBlob(ctx, digest1)
	assert.Equal(t, err, nil)
	assert.Equal(t, string(data), "this is a test")
}
