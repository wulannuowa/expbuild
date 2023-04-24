package client

import (
	"context"
	"fmt"

	pbbs "github.com/expbuild/expbuild/pkg/proto/gen/bytestream"
	pb "github.com/expbuild/expbuild/pkg/proto/gen/remote_execution"
	"github.com/expbuild/expbuild/pkg/util/log"
	"google.golang.org/grpc"
)

type Client struct {
	InstanceName  string
	actionCache   pb.ActionCacheClient
	byteStream    pbbs.ByteStreamClient
	cas           pb.ContentAddressableStorageClient
	execution     pb.ExecutionClient
	Connection    *grpc.ClientConn
	CASConnection *grpc.ClientConn
}

type DialParams struct {
	// Service contains the address of remote execution service.
	Service string

	// CASService contains the address of the CAS service, if it is separate from
	// the remote execution service.
	CASService string
}

// Close closes the underlying gRPC connection(s).
func (c *Client) Close() error {

	err := c.Connection.Close()
	if err != nil {
		return err
	}
	if c.CASConnection != c.Connection {
		return c.CASConnection.Close()
	}
	return nil
}

func NewClient(ctx context.Context, instanceName string, params DialParams) (*Client, error) {

	if params.Service == "" {
		return nil, fmt.Errorf("service is null")
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(params.Service, opts...)
	casConn := conn
	if params.CASService != "" && params.CASService != params.Service {
		log.Infof("Connecting to CAS service %s", params.Service)
		casConn, err = grpc.Dial(params.CASService, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %v", err)
	}
	client := &Client{

		InstanceName:  instanceName,
		actionCache:   pb.NewActionCacheClient(conn),
		byteStream:    pbbs.NewByteStreamClient(conn),
		cas:           pb.NewContentAddressableStorageClient(casConn),
		execution:     pb.NewExecutionClient(conn),
		Connection:    conn,
		CASConnection: casConn,
	}
	return client, nil
}
