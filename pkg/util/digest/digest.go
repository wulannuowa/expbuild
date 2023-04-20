package digest

import (
	"strconv"
	"strings"

	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
)

// DigestToString
// convert `pb.digest` to string format `hash_length`
func DigestToString(digest *pb.Digest) string {
	return digest.Hash + "_" + strconv.FormatInt(int64(digest.SizeBytes), 10)
}

// StringToDigest
// convert `digest_length` to `pb.digest`
func StringToDigest(str string) *pb.Digest {
	digest := &pb.Digest{}
	parts := strings.Split(str, "_")
	if len(parts) < 2 {
		return digest
	}
	digest.Hash = parts[0]
	digest.SizeBytes, _ = strconv.ParseInt(parts[1], 10, 64)
	return digest
}

// get digest from resource name in format `/[instance]/blobs/[digest]/[len]`
func GetDigestFromResourceName(resourceName string) *pb.Digest {
	items := strings.Split(resourceName, "/")
	hash := items[len(items)-2]
	byte_size_str := items[len(items)-1]
	byte_size, _ := strconv.Atoi(byte_size_str)
	digest := pb.Digest{
		Hash:      hash,
		SizeBytes: int64(byte_size),
	}
	return &digest
}
