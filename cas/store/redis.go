package store

import (
	"context"
	"flag"
	"log"

	pb "github.com/expbuild/expbuild/proto/gen/remote_execution"
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

func (s RedisStore) HasBlob(digest *pb.Digest) bool {
	var ctx = context.Background()
	n, err := s.rdb.Exists(ctx, digest.Hash).Result()
	if err != nil {
		log.Fatalf("some thing getting error %v", err)
	}
	return n > 0
}

func (s RedisStore) GetBlob(digest *pb.Digest) ([]byte, error) {
	data := []byte{}
	return data, nil
}
