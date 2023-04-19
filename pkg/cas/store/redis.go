package store

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"github.com/go-redis/redis/v8"
)

var (
	addr     = flag.String("store.redis.addr", "localhost:6379", "The redis address of cas")
	password = flag.String("store.redis.password", "", "The redis password of cas")
	db       = flag.Int("store.redis.db", 0, "The redis db of cas")
)

type RedisStore struct {
	rdb *redis.Client
}

func MakeRedisStore() RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:     *addr,
		Password: *password,
		DB:       *db,
	})
	return RedisStore{
		rdb: client,
	}
}

func (s RedisStore) HasBlob(ctx context.Context, digest *pb.Digest) bool {
	n, err := s.rdb.Exists(ctx, digest.Hash).Result()
	if err != nil {
		log.Errorf("some thing getting error %v", err)
	}
	return n > 0
}

func (s RedisStore) GetBlob(ctx context.Context, digest *pb.Digest) ([]byte, error) {
	return s.rdb.Get(ctx, digestToKey(digest)).Bytes()
}

func (s RedisStore) PutBlob(ctx context.Context, digest *pb.Digest, data []byte) error {
	return s.rdb.Set(ctx, digestToKey(digest), data, 0).Err()
}

func (s RedisStore) FindMissingBlobs(ctx context.Context, digests []*pb.Digest) ([]*pb.Digest, error) {

	pipe := s.rdb.Pipeline()
	pipe_result := map[string]*redis.IntCmd{}
	for _, digest := range digests {
		key := digestToKey(digest)
		pipe_result[key] = pipe.Exists(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Errorf("redis error %v", err)
		return []*pb.Digest{}, err
	}

	missing_blobs := []*pb.Digest{}
	for k, v := range pipe_result {
		n, err := v.Result()
		if err == nil && n == 0 {
			missing_blobs = append(missing_blobs, keyToDigest(k))
		}
	}
	return missing_blobs, nil
}

func digestToKey(digest *pb.Digest) string {
	return fmt.Sprintf("cas_%s_%d", digest.Hash, digest.SizeBytes)
}

func keyToDigest(key string) *pb.Digest {
	items := strings.Split(key, "_")
	if len(items) < 3 {
		log.Errorf("key format error")
		return &pb.Digest{}
	}
	length, err := strconv.Atoi(items[2])
	if err != nil {
		length = 0
	}
	return &pb.Digest{
		Hash:      items[1],
		SizeBytes: int64(length),
	}
}
