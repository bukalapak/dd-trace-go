// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

// Package redis provides tracing functions for tracing the go-redis/redis package (https://github.com/go-redis/redis).
package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/go-redis/redis"
)

// ClusterClient is used to trace requests to a redis cluster.
type ClusterClient struct {
	*redis.ClusterClient
	*clusterparams

	mu  sync.RWMutex // guards ctx
	ctx context.Context
}

var _ redis.Cmdable = (*ClusterClient)(nil)

// ClusterPipeliner is used to trace pipelines executed on a Redis Cluster.
type ClusterPipeliner struct {
	redis.Pipeliner
	*clusterparams

	ctx context.Context
}

var _ redis.Pipeliner = (*Pipeliner)(nil)

// params holds the tracer and a set of parameters which are recorded with every trace.
type clusterparams struct {
	host   []string
	port   []string
	//db     string // Could not find DB
	config *clientConfig
}

// NewClient returns a new Client that is traced with the default tracer under
// the service name "redis".
func NewClusterClient(opt *redis.ClusterOptions, opts ...ClientOption) *ClusterClient {
	return WrapClusterClient(redis.NewClusterClient(opt), opts...)
}

// WrapClient wraps a given redis.Client with a tracer under the given service name.
func WrapClusterClient(c *redis.ClusterClient, opts ...ClientOption) *ClusterClient {
	cfg := new(clientConfig)
	defaults(cfg)
	for _, fn := range opts {
		fn(cfg)
	}
	opt := c.Options()
  hosts := make([]string, len(opt.Addrs))
  ports := make([]string, len(opt.Addrs))
  for i, addr := range opt.Addrs {
	  host, port, err := net.SplitHostPort(addr)
	  if err != nil {
		  host = addr
		  port = "6379"
	  }
    hosts[i] = host
    ports[i] = port
  }
	params := &clusterparams{
		host:   hosts,
		port:   ports,
//		db:     strconv.Itoa(opt.DB),
		config: cfg,
	}
	tc := &ClusterClient{ClusterClient: c, clusterparams: params}
	tc.ClusterClient.WrapProcess(createWrapperFromClusterClient(tc))
	return tc
}

//TODO(Bagus): Find a way to implement pipelining for Cluster
// Pipeline creates a Pipeline from a Client
//func (c *ClusterClient) Pipeline() redis.Pipeliner {
//	c.mu.RLock()
//	ctx := c.ctx
//	c.mu.RUnlock()
//	return &Pipeliner{c.ClusterClient.Pipeline(), c.clusterparams, ctx}
//}

// WithContext sets a context on a ClusterClient. Use it to ensure that emitted spans have the correct parent.
func (c *ClusterClient) WithContext(ctx context.Context) *ClusterClient {
	c.mu.Lock()
	c.ctx = ctx
	c.mu.Unlock()
	return c
}

// Context returns the active context in the ClusterClient.
func (c *ClusterClient) Context() context.Context {
	c.mu.RLock()
	ctx := c.ctx
	c.mu.RUnlock()
	return ctx
}

// createWrapperFromClusterClient returns a new createWrapper function which wraps the processor with tracing
// information obtained from the provided ClusterClient. To understand this functionality better see the
// documentation for the github.com/go-redis/redis.(*baseClient).WrapProcess function.
func createWrapperFromClusterClient(tc *ClusterClient) func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
	return func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			tc.mu.RLock()
			ctx := tc.ctx
			tc.mu.RUnlock()
			fmt.Println("%v", cmd.Args())
      raw := cmderToString(cmd)
			parts := strings.Split(raw, " ")
			length := len(parts) - 1
			p := tc.clusterparams
			opts := []ddtrace.StartSpanOption{
				tracer.SpanType(ext.SpanTypeRedis),
				tracer.ServiceName(p.config.serviceName),
				tracer.ResourceName(parts[0]),
				tracer.Tag(ext.TargetHost, p.host),
				tracer.Tag(ext.TargetPort, p.port),
	//			tracer.Tag("out.db", p.db),
				tracer.Tag("redis.raw_command", raw),
				tracer.Tag("redis.args_length", strconv.Itoa(length)),
			}
			if rate := p.config.analyticsRate; rate > 0 {
				opts = append(opts, tracer.Tag(ext.EventSampleRate, rate))
			}
			span, _ := tracer.StartSpanFromContext(ctx, "redis.command", opts...)
			err := oldProcess(cmd)
			var finishOpts []ddtrace.FinishOption
			if err != redis.Nil {
				finishOpts = append(finishOpts, tracer.WithError(err))
			}
			span.Finish(finishOpts...)
			return err
		}
	}
}

