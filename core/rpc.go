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
	log.Printf(n.Id + " has started gRPC server")
}

func (n *Node) StartClients() {
	for key, addr := range n.Peers {
		var conn *grpc.ClientConn
		var err error
		for {
			conn, err = grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)

			if err == nil {
				break
			}
		}

		client := pb.NewNodeClient(conn)
		n.Clients[key] = client
	}

	log.Printf("%s successfully connected to %d peers", n.Id, len(n.Peers))
}
