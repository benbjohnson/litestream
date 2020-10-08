package main

/*
import (
	"context"
	"io"
	"sync"

	"bazil.org/fuse"
)

type Server struct {
	wg sync.WaitGroup

	SourcePath string
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Close() error {
	s.wg.Wait()
	return nil
}

func (s *Server) Serve(ctx context.Context, conn *fuse.Conn) error {
	for {
		r, err := conn.ReadRequest()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handleRequest(ctx, r) }()
	}
}

func (s *Server) handleRequest(ctx context.Context, r fuse.Request) {
	switch r := r.(type) {
	case *fuse.AccessRequest:
		s.handleAccessRequest(ctx, r)
	case *fuse.BatchForgetRequest:
		s.handleBatchForgetRequest(ctx, r)
	case *fuse.CreateRequest:
		s.handleCreateRequest(ctx, r)
	case *fuse.DestroyRequest:
		s.handleDestroyRequest(ctx, r)
	case *fuse.ExchangeDataRequest:
		s.handleExchangeDataRequest(ctx, r)
	case *fuse.FlushRequest:
		s.handleFlushRequest(ctx, r)
	case *fuse.ForgetRequest:
		s.handleForgetRequest(ctx, r)
	case *fuse.FsyncRequest:
		s.handleFsyncRequest(ctx, r)
	case *fuse.GetattrRequest:
		s.handleGetattrRequest(ctx, r)
	case *fuse.GetxattrRequest:
		s.handleGetxattrRequest(ctx, r)
	case *fuse.InterruptRequest:
		s.handleInterruptRequest(ctx, r)
	case *fuse.LinkRequest:
		s.handleLinkRequest(ctx, r)
	case *fuse.ListxattrRequest:
		s.handleListxattrRequest(ctx, r)
	case *fuse.LockRequest:
		s.handleLockRequest(ctx, r)
	case *fuse.LookupRequest:
		s.handleLookupRequest(ctx, r)
	case *fuse.MkdirRequest:
		s.handleMkdirRequest(ctx, r)
	case *fuse.MknodRequest:
		s.handleMknodRequest(ctx, r)
	case *fuse.OpenRequest:
		s.handleOpenRequest(ctx, r)
	case *fuse.PollRequest:
		s.handlePollRequest(ctx, r)
	case *fuse.QueryLockRequest:
		s.handleQueryLockRequest(ctx, r)
	case *fuse.ReadRequest:
		s.handleReadRequest(ctx, r)
	case *fuse.ReadlinkRequest:
		s.handleReadlinkRequest(ctx, r)
	case *fuse.ReleaseRequest:
		s.handleReleaseRequest(ctx, r)
	case *fuse.RemoveRequest:
		s.handleRemoveRequest(ctx, r)
	case *fuse.RemovexattrRequest:
		s.handleRemovexattrRequest(ctx, r)
	case *fuse.RenameRequest:
		s.handleRenameRequest(ctx, r)
	case *fuse.SetattrRequest:
		s.handleSetattrRequest(ctx, r)
	case *fuse.SetxattrRequest:
		s.handleSetxattrRequest(ctx, r)
	case *fuse.StatfsRequest:
		s.handleStatfsRequest(ctx, r)
	case *fuse.SymlinkRequest:
		s.handleSymlinkRequest(ctx, r)
	case *fuse.UnrecognizedRequest:
		s.handleUnrecognizedRequest(ctx, r)
	case *fuse.WriteRequest:
		s.handleWriteRequest(ctx, r)
	}
}

func (s *Server) handleAccessRequest(ctx context.Context, r *fuse.AccessRequest) {
	panic("TODO")
}

func (s *Server) handleBatchForgetRequest(ctx context.Context, r *fuse.BatchForgetRequest) {
	panic("TODO")
}

func (s *Server) handleCreateRequest(ctx context.Context, r *fuse.CreateRequest) {
	panic("TODO")
}

func (s *Server) handleDestroyRequest(ctx context.Context, r *fuse.DestroyRequest) {
	panic("TODO")
}

func (s *Server) handleExchangeDataRequest(ctx context.Context, r *fuse.ExchangeDataRequest) {
	panic("TODO")
}

func (s *Server) handleFlushRequest(ctx context.Context, r *fuse.FlushRequest) {
	panic("TODO")
}

func (s *Server) handleForgetRequest(ctx context.Context, r *fuse.ForgetRequest) {
	panic("TODO")
}

func (s *Server) handleFsyncRequest(ctx context.Context, r *fuse.FsyncRequest) {
	panic("TODO")
}

func (s *Server) handleGetattrRequest(ctx context.Context, r *fuse.GetattrRequest) {
	panic("TODO")
}

func (s *Server) handleGetxattrRequest(ctx context.Context, r *fuse.GetxattrRequest) {
	panic("TODO")
}

func (s *Server) handleInterruptRequest(ctx context.Context, r *fuse.InterruptRequest) {
	panic("TODO")
}

func (s *Server) handleLinkRequest(ctx context.Context, r *fuse.LinkRequest) {
	panic("TODO")
}

func (s *Server) handleListxattrRequest(ctx context.Context, r *fuse.ListxattrRequest) {
	panic("TODO")
}

func (s *Server) handleLockRequest(ctx context.Context, r *fuse.LockRequest) {
	panic("TODO")
}

func (s *Server) handleLookupRequest(ctx context.Context, r *fuse.LookupRequest) {
	panic("TODO")
}

func (s *Server) handleMkdirRequest(ctx context.Context, r *fuse.MkdirRequest) {
	panic("TODO")
}

func (s *Server) handleMknodRequest(ctx context.Context, r *fuse.MknodRequest) {
	panic("TODO")
}

func (s *Server) handleOpenRequest(ctx context.Context, r *fuse.OpenRequest) {
	panic("TODO")
}

func (s *Server) handlePollRequest(ctx context.Context, r *fuse.PollRequest) {
	panic("TODO")
}

func (s *Server) handleQueryLockRequest(ctx context.Context, r *fuse.QueryLockRequest) {
	panic("TODO")
}

func (s *Server) handleReadRequest(ctx context.Context, r *fuse.ReadRequest) {
	panic("TODO")
}

func (s *Server) handleReadlinkRequest(ctx context.Context, r *fuse.ReadlinkRequest) {
	panic("TODO")
}

func (s *Server) handleReleaseRequest(ctx context.Context, r *fuse.ReleaseRequest) {
	panic("TODO")
}

func (s *Server) handleRemoveRequest(ctx context.Context, r *fuse.RemoveRequest) {
	panic("TODO")
}

func (s *Server) handleRemovexattrRequest(ctx context.Context, r *fuse.RemovexattrRequest) {
	panic("TODO")
}

func (s *Server) handleRenameRequest(ctx context.Context, r *fuse.RenameRequest) {
	panic("TODO")
}

func (s *Server) handleSetattrRequest(ctx context.Context, r *fuse.SetattrRequest) {
	panic("TODO")
}

func (s *Server) handleSetxattrRequest(ctx context.Context, r *fuse.SetxattrRequest) {
	panic("TODO")
}

func (s *Server) handleStatfsRequest(ctx context.Context, r *fuse.StatfsRequest) {
	panic("TODO")
}

func (s *Server) handleSymlinkRequest(ctx context.Context, r *fuse.SymlinkRequest) {
	panic("TODO")
}

func (s *Server) handleUnrecognizedRequest(ctx context.Context, r *fuse.UnrecognizedRequest) {
	panic("TODO")
}

func (s *Server) handleWriteRequest(ctx context.Context, r *fuse.WriteRequest) {
	panic("TODO")
}
*/
