package server

import (
	"context"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(record *log_v1.Record) (uint64, error)
	Read(uint642 uint64) (*log_v1.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

var _ log_v1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

func newServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (gSrv *grpc.Server, err error) {
	gSrv = grpc.NewServer(opts...)
	srv, err := newServer(config)
	if err != nil {
		return nil, err
	}
	log_v1.RegisterLogServer(gSrv, srv)
	return gSrv, err
}

func (srv *grpcServer) Produce(ctx context.Context, produceRequest *log_v1.ProduceRequest) (*log_v1.ProduceResponse, error) {
	off, err := srv.CommitLog.Append(produceRequest.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: off}, nil
}

func (srv *grpcServer) Consume(ctx context.Context, consumeRequest *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	record, err := srv.CommitLog.Read(consumeRequest.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (srv *grpcServer) ProduceStream(stream log_v1.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := srv.Produce(stream.Context(), req)

		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (srv *grpcServer) ConsumeStream(req *log_v1.ConsumeRequest, stream log_v1.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := srv.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case log_v1.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
