package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"

	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
)

const (
	testBufSize = 1024 * 1024
)

var (
	testClient Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, f := getTestClient(ctx)
	testClient = c
	r := m.Run()
	f()
	os.Exit(r)
}

func TestNewClient(t *testing.T) {
	t.Run("no arg for with port", func(t *testing.T) {
		_, err := NewClientWithPort("")
		assert.Error(t, err)
	})

	t.Run("no arg for with address", func(t *testing.T) {
		_, err := NewClientWithAddress("")
		assert.Error(t, err)
	})

	t.Run("new client closed with empty token", func(t *testing.T) {
		c, err := NewClient()
		assert.NoError(t, err)
		defer c.Close()
		c.WithAuthToken("")
	})

	t.Run("new client with trace ID", func(t *testing.T) {
		c, err := NewClient()
		assert.NoError(t, err)
		defer c.Close()
		ctx := c.WithTraceID(context.Background(), "")
		_ = c.WithTraceID(ctx, "test")
	})
}

func TestShutdown(t *testing.T) {
	ctx := context.Background()

	t.Run("shutdown", func(t *testing.T) {
		err := testClient.Shutdown(ctx)
		assert.NoError(t, err)
	})
}

func getTestClient(ctx context.Context) (client Client, closer func()) {
	s := grpc.NewServer()
	pb.RegisterDaprServer(s, &testDaprServer{
		state: make(map[string][]byte),
	})

	l := bufconn.Listen(testBufSize)
	go func() {
		if err := s.Serve(l); err != nil && err.Error() != "closed" {
			logger.Fatalf("test server exited with error: %v", err)
		}
	}()

	d := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return l.Dial()
	})

	c, err := grpc.DialContext(ctx, "", d, grpc.WithInsecure())
	if err != nil {
		logger.Fatalf("failed to dial test context: %v", err)
	}

	closer = func() {
		l.Close()
		s.Stop()
	}

	client = NewClientWithConnection(c)
	return
}

type testDaprServer struct {
	pb.UnimplementedDaprServer
	state map[string][]byte
}

func (s *testDaprServer) InvokeService(ctx context.Context, req *pb.InvokeServiceRequest) (*commonv1pb.InvokeResponse, error) {
	if req.Message == nil {
		return &commonv1pb.InvokeResponse{
			ContentType: "text/plain",
			Data: &anypb.Any{
				Value: []byte("pong"),
			},
		}, nil
	}
	return &commonv1pb.InvokeResponse{
		ContentType: req.Message.ContentType,
		Data:        req.Message.Data,
	}, nil
}

func (s *testDaprServer) GetState(ctx context.Context, req *pb.GetStateRequest) (*pb.GetStateResponse, error) {
	return &pb.GetStateResponse{
		Data: s.state[req.Key],
		Etag: "1",
	}, nil
}

func (s *testDaprServer) GetBulkState(ctx context.Context, in *pb.GetBulkStateRequest) (*pb.GetBulkStateResponse, error) {
	items := make([]*pb.BulkStateItem, 0)
	for _, k := range in.GetKeys() {
		if v, found := s.state[k]; found {
			item := &pb.BulkStateItem{
				Key:  k,
				Etag: "1",
				Data: v,
			}
			items = append(items, item)
		}
	}
	return &pb.GetBulkStateResponse{
		Items: items,
	}, nil
}

func (s *testDaprServer) SaveState(ctx context.Context, req *pb.SaveStateRequest) (*empty.Empty, error) {
	for _, item := range req.States {
		s.state[item.Key] = item.Value
	}
	return &empty.Empty{}, nil
}

func (s *testDaprServer) DeleteState(ctx context.Context, req *pb.DeleteStateRequest) (*empty.Empty, error) {
	delete(s.state, req.Key)
	return &empty.Empty{}, nil
}

func (s *testDaprServer) DeleteBulkState(ctx context.Context, req *pb.DeleteBulkStateRequest) (*empty.Empty, error) {
	for _, item := range req.States {
		delete(s.state, item.Key)
	}
	return &empty.Empty{}, nil
}

func (s *testDaprServer) ExecuteStateTransaction(ctx context.Context, in *pb.ExecuteStateTransactionRequest) (*empty.Empty, error) {
	for _, op := range in.GetOperations() {
		item := op.GetRequest()
		switch opType := op.GetOperationType(); opType {
		case "upsert":
			s.state[item.Key] = item.Value
		case "delete":
			delete(s.state, item.Key)
		default:
			return &empty.Empty{}, fmt.Errorf("invalid operation type: %s", opType)
		}
	}
	return &empty.Empty{}, nil
}

func (s *testDaprServer) PublishEvent(ctx context.Context, req *pb.PublishEventRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *testDaprServer) InvokeBinding(ctx context.Context, req *pb.InvokeBindingRequest) (*pb.InvokeBindingResponse, error) {
	if req.Data == nil {
		return &pb.InvokeBindingResponse{
			Data:     []byte("test"),
			Metadata: map[string]string{"k1": "v1", "k2": "v2"},
		}, nil
	}
	return &pb.InvokeBindingResponse{
		Data:     req.Data,
		Metadata: req.Metadata,
	}, nil
}

func (s *testDaprServer) GetSecret(ctx context.Context, req *pb.GetSecretRequest) (*pb.GetSecretResponse, error) {
	d := make(map[string]string)
	d["test"] = "value"
	return &pb.GetSecretResponse{
		Data: d,
	}, nil
}

func (s *testDaprServer) GetBulkSecret(ctx context.Context, req *pb.GetBulkSecretRequest) (*pb.GetBulkSecretResponse, error) {
	d := make(map[string]*pb.SecretResponse)
	d["test"] = &pb.SecretResponse{
		Secrets: map[string]string{
			"test": "value",
		},
	}
	return &pb.GetBulkSecretResponse{
		Data: d,
	}, nil
}

func (s *testDaprServer) InvokeActor(context.Context, *pb.InvokeActorRequest) (*pb.InvokeActorResponse, error) {
	return nil, errors.New("actors not implemented in go SDK")
}

func (s *testDaprServer) RegisterActorTimer(context.Context, *pb.RegisterActorTimerRequest) (*empty.Empty, error) {
	return nil, errors.New("actors not implemented in go SDK")
}

func (s *testDaprServer) UnregisterActorTimer(context.Context, *pb.UnregisterActorTimerRequest) (*empty.Empty, error) {
	return nil, errors.New("actors not implemented in go SDK")
}

func (s *testDaprServer) Shutdown(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
