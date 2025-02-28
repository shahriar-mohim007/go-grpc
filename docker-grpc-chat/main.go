package main

import (
	"log"
	"net"
	"os"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"grpc-chat/proto"
)

var grpcLog grpclog.LoggerV2

func init() {
	grpcLog = grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Connection struct {
	stream proto.Broadcast_CreateStreamServer
	id     string
	active bool
	error  chan error
}

type Server struct {
	mu         sync.Mutex
	Connection []*Connection
	proto.UnimplementedBroadcastServer
}

func (s *Server) CreateStream(pconn *proto.Connect, stream proto.Broadcast_CreateStreamServer) error {
	conn := &Connection{
		stream: stream,
		id:     pconn.User.Id,
		active: true,
		error:  make(chan error),
	}

	// Prevent race condition while modifying s.Connection
	s.mu.Lock()
	s.Connection = append(s.Connection, conn)
	s.mu.Unlock()

	grpcLog.Infof("New connection established: %s", conn.id)
	return <-conn.error
}

func (s *Server) BroadcastMessage(ctx context.Context, msg *proto.Message) (*proto.Close, error) {
	s.mu.Lock()
	connections := make([]*Connection, len(s.Connection))
	copy(connections, s.Connection)
	s.mu.Unlock()

	if len(connections) == 0 {
		grpcLog.Warning("No active connections to broadcast message")
		return &proto.Close{}, nil
	}

	wait := sync.WaitGroup{}
	done := make(chan struct{})

	for _, conn := range connections {
		wait.Add(1)

		go func(msg *proto.Message, conn *Connection) {
			defer wait.Done()

			if conn.active {
				err := conn.stream.Send(msg)
				grpcLog.Infof("Sending message to: %s", conn.id)

				if err != nil {
					grpcLog.Errorf("Error sending to %s: %v", conn.id, err)
					conn.active = false
					conn.error <- err
				}
			}
		}(msg, conn)
	}

	go func() {
		wait.Wait()
		close(done)
	}()

	<-done
	return &proto.Close{}, nil
}

func main() {
	server := &Server{}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error creating the server: %v", err)
	}

	grpcLog.Info("Starting server at port :8080")
	proto.RegisterBroadcastServer(grpcServer, server)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
