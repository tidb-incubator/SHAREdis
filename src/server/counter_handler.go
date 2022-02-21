package server

import (
	"context"
	. "sharedis/thrift/gen-go/sharestore"
)

// Parameters:
//  - Request
func (s *CmdHandler) IncrCounter(ctx context.Context, request *IncrCounterRequest) (r *IncrCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) IncrCounterTtl(ctx context.Context, request *IncrCounterTtlRequest) (r *IncrCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) MultiIncrCounter(ctx context.Context, request *MultiIncrCounterRequest) (r *MultiIncrCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) MultiIncrCounterTtl(ctx context.Context, request *MultiIncrCounterTtlRequest) (r *MultiIncrCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) SetCounter(ctx context.Context, request *SetCounterRequest) (r *SetCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) SetCounterTtl(ctx context.Context, request *SetCounterTtlRequest) (r *SetCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) MultiSetCounter(ctx context.Context, request *MultiSetCounterRequest) (r *MultiSetCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) MultiSetCounterTtl(ctx context.Context, request *MultiSetCounterTtlRequest) (r *MultiSetCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Request
func (s *CmdHandler) GetCounter(ctx context.Context, request *GetCounterRequest) (r *GetCounterResponse, err error) {
	return nil, nil
}

// Parameters:
//  - Mrequest
func (s *CmdHandler) MultiGetCounter(ctx context.Context, mrequest *MultiGetCounterRequest) (r *MultiGetCounterResponse, err error) {
	return nil, nil
}
