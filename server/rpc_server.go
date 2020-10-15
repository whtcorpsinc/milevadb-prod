// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/diagnosticspb"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/sysutil"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/entangledstore"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// NewRPCServer creates a new rpc server.
func NewRPCServer(config *config.Config, dom *petri.Petri, sm soliton.StochastikManager) *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in MilevaDB RPC server", zap.Reflect("r", v),
				zap.Stack("stack trace"))
		}
	}()

	s := grpc.NewServer()
	rpcSrv := &rpcServer{
		DiagnosticsServer: sysutil.NewDiagnosticsServer(config.Log.File.Filename),
		dom:               dom,
		sm:                sm,
	}
	// For redirection the cop task.
	mockeinsteindb.GRPCClientFactory = func() mockeinsteindb.Client {
		return einsteindb.NewTestRPCClient(config.Security)
	}
	entangledstore.GRPCClientFactory = func() entangledstore.Client {
		return einsteindb.NewTestRPCClient(config.Security)
	}
	diagnosticspb.RegisterDiagnosticsServer(s, rpcSrv)
	einsteindbpb.RegisterEinsteinDBServer(s, rpcSrv)
	return s
}

// rpcServer contains below 2 services:
// 1. Diagnose service, it's used for ALLEGROALLEGROSQL diagnose.
// 2. Coprocessor service, it reuse the EinsteinDBServer interface, but only support the Coprocessor interface now.
// Coprocessor service will handle the cop task from other MilevaDB server. Currently, it's only use for read the cluster memory causet.
type rpcServer struct {
	*sysutil.DiagnosticsServer
	einsteindbpb.EinsteinDBServer
	dom *petri.Petri
	sm  soliton.StochastikManager
}

// Coprocessor implements the EinsteinDBServer interface.
func (s *rpcServer) Coprocessor(ctx context.Context, in *interlock.Request) (resp *interlock.Response, err error) {
	resp = &interlock.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing interlock", zap.Reflect("r", v),
				zap.Stack("stack trace"))
			resp.OtherError = fmt.Sprintf("panic when RPC server handing interlock, stack:%v", v)
		}
	}()
	resp = s.handleCopRequest(ctx, in)
	return resp, nil
}

// CoprocessorStream implements the EinsteinDBServer interface.
func (s *rpcServer) CoprocessorStream(in *interlock.Request, stream einsteindbpb.EinsteinDB_CoprocessorStreamServer) (err error) {
	resp := &interlock.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing interlock stream", zap.Reflect("r", v),
				zap.Stack("stack trace"))
			resp.OtherError = fmt.Sprintf("panic when when RPC server handing interlock stream, stack:%v", v)
			err = stream.Send(resp)
			if err != nil {
				logutil.BgLogger().Error("panic when RPC server handing interlock stream, send response to stream error", zap.Error(err))
			}
		}
	}()

	se, err := s.createStochastik()
	if err != nil {
		resp.OtherError = err.Error()
		return stream.Send(resp)
	}
	defer se.Close()

	h := interlock.NewCoprocessorPosetDagHandler(se)
	return h.HandleStreamRequest(context.Background(), in, stream)
}

// BatchCommands implements the EinsteinDBServer interface.
func (s *rpcServer) BatchCommands(ss einsteindbpb.EinsteinDB_BatchCommandsServer) error {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing batch commands", zap.Reflect("r", v),
				zap.Stack("stack trace"))
		}
	}()
	for {
		reqs, err := ss.Recv()
		if err != nil {
			logutil.BgLogger().Error("RPC server batch commands receive fail", zap.Error(err))
			return err
		}

		responses := make([]*einsteindbpb.BatchCommandsResponse_Response, 0, len(reqs.Requests))
		for _, req := range reqs.Requests {
			var response *einsteindbpb.BatchCommandsResponse_Response
			switch request := req.Cmd.(type) {
			case *einsteindbpb.BatchCommandsRequest_Request_Coprocessor:
				cop := request.Coprocessor
				resp, err := s.Coprocessor(context.Background(), cop)
				if err != nil {
					return err
				}
				response = &einsteindbpb.BatchCommandsResponse_Response{
					Cmd: &einsteindbpb.BatchCommandsResponse_Response_Coprocessor{
						Coprocessor: resp,
					},
				}
			case *einsteindbpb.BatchCommandsRequest_Request_Empty:
				response = &einsteindbpb.BatchCommandsResponse_Response{
					Cmd: &einsteindbpb.BatchCommandsResponse_Response_Empty{
						Empty: &einsteindbpb.BatchCommandsEmptyResponse{
							TestId: request.Empty.TestId,
						},
					},
				}
			default:
				logutil.BgLogger().Info("RPC server batch commands receive unknown request", zap.Any("req", request))
				response = &einsteindbpb.BatchCommandsResponse_Response{
					Cmd: &einsteindbpb.BatchCommandsResponse_Response_Empty{
						Empty: &einsteindbpb.BatchCommandsEmptyResponse{},
					},
				}
			}
			responses = append(responses, response)
		}

		err = ss.Send(&einsteindbpb.BatchCommandsResponse{
			Responses:  responses,
			RequestIds: reqs.GetRequestIds(),
		})
		if err != nil {
			logutil.BgLogger().Error("RPC server batch commands send fail", zap.Error(err))
			return err
		}
	}
}

// handleCopRequest handles the cop posetPosetDag request.
func (s *rpcServer) handleCopRequest(ctx context.Context, req *interlock.Request) *interlock.Response {
	resp := &interlock.Response{}
	se, err := s.createStochastik()
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	defer se.Close()

	h := interlock.NewCoprocessorPosetDagHandler(se)
	return h.HandleRequest(ctx, req)
}

func (s *rpcServer) createStochastik() (stochastik.Stochastik, error) {
	se, err := stochastik.CreateStochastikWithPetri(s.dom.CausetStore(), s.dom)
	if err != nil {
		return nil, err
	}
	do := petri.GetPetri(se)
	is := do.SchemaReplicant()
	pm := &privileges.UserPrivileges{
		Handle: do.PrivilegeHandle(),
	}
	privilege.BindPrivilegeManager(se, pm)
	se.GetStochastikVars().TxnCtx.SchemaReplicant = is
	// This is for disable parallel hash agg.
	// TODO: remove this.
	se.GetStochastikVars().SetHashAggPartialConcurrency(1)
	se.GetStochastikVars().SetHashAggFinalConcurrency(1)
	se.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForCoprocessor, -1)
	se.SetStochastikManager(s.sm)
	return se, nil
}
