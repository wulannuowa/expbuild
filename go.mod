module github.com/expbuild/expbuild

go 1.19

replace github.com/bazelbuild/remote-apis/build/bazel/semver => github.com/expbuild/expbuild/gen/proto/build/bazel/semver v0.0.1

require (
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/rabbitmq/amqp091-go v1.5.0
	github.com/rs/zerolog v1.28.0
	github.com/stretchr/testify v1.8.1
	google.golang.org/genproto v0.0.0-20221207170731-23e4bf6bdc37
	google.golang.org/grpc v1.52.3
	google.golang.org/protobuf v1.28.1
)

require (
	cloud.google.com/go/longrunning v0.3.0 // indirect
	github.com/alicebob/gopher-json v0.0.0-20230218143504-906a9b012302 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gomodule/redigo v1.8.9 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
