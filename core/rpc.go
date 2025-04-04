package core

import (
	"log"
	"net"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedNodeServer
}

func (n *Node) StartServer() {
	lis, _ := net.Listen("tcp", ":"+n.Port)
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServer(grpcServer, &server{})
	grpcServer.Serve(lis)
}

func (n *Node) StartClients() {
	for key, addr := range n.Peers {

		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}

		client := pb.NewNodeClient(conn)
		n.Clients[key] = client
	}
}
