/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	mylog "github.com/containerd/accelerated-container-image/internal/log"
	"github.com/containerd/accelerated-container-image/pkg/metrics"
	overlaybd "github.com/containerd/accelerated-container-image/pkg/snapshot"
	"github.com/containerd/accelerated-container-image/pkg/tracing"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const defaultConfigPath = "/etc/overlaybd-snapshotter/config.json"

var pconfig *overlaybd.BootConfig
var commitID string = "unknown"

func requestIDInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	requestID := tracing.GetRequestID(ctx)
	if requestID == "" {
		requestID = mylog.GenerateRequestID()
		ctx = tracing.SetRequestID(ctx, requestID)
	}

	ctx = mylog.WithRequestID(ctx, requestID)
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("req_id", requestID))

	return handler(ctx, req)
}

func otelLoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Create temporary file to store OTEL information
	tmpFile, err := os.CreateTemp("", "otel_trace_*.log")
	if err != nil {
		logrus.Errorf("Failed to create temp file for OTEL data: %v", err)
	} else {
		defer tmpFile.Close()

		// Extract trace context
		span := trace.SpanFromContext(ctx)
		spanContext := span.SpanContext()

		// Extract baggage
		bag := baggage.FromContext(ctx)

		// Extract GRPC metadata for OTEL headers
		md, _ := metadata.FromIncomingContext(ctx)

		// Format OTEL information
		otelInfo := fmt.Sprintf("=== OpenTelemetry Trace Data for %s ===\n", info.FullMethod)
		otelInfo += fmt.Sprintf("Timestamp: %s\n", time.Now().Format(time.RFC3339))
		otelInfo += fmt.Sprintf("Method: %s\n", info.FullMethod)
		otelInfo += fmt.Sprintf("Service: %s\n", os.Getenv("OTEL_SERVICE_NAME"))
		otelInfo += "\n--- Trace Context ---\n"

		if spanContext.IsValid() {
			otelInfo += fmt.Sprintf("Trace ID: %s\n", spanContext.TraceID().String())
			otelInfo += fmt.Sprintf("Span ID: %s\n", spanContext.SpanID().String())
			otelInfo += fmt.Sprintf("Trace Flags: %s\n", spanContext.TraceFlags().String())
			otelInfo += fmt.Sprintf("Trace State: %s\n", spanContext.TraceState().String())
			otelInfo += fmt.Sprintf("Is Remote: %t\n", spanContext.IsRemote())
		} else {
			otelInfo += "No valid trace context found\n"
		}

		// Add baggage information
		otelInfo += "\n--- Baggage ---\n"
		baggageMembers := bag.Members()
		if len(baggageMembers) > 0 {
			for _, member := range baggageMembers {
				otelInfo += fmt.Sprintf("  %s: %s\n", member.Key(), member.Value())
			}
			// Also extract the request ID specifically using the project's tracing helper
			requestID := tracing.GetRequestID(ctx)
			if requestID != "" {
				otelInfo += fmt.Sprintf("  [project-specific] request.id: %s\n", requestID)
			}
		} else {
			otelInfo += "No baggage found\n"
		}

		// Add OTEL-specific headers from GRPC metadata
		otelInfo += "\n--- OTEL Headers ---\n"
		otelHeaders := []string{"traceparent", "tracestate", "baggage", "b3", "x-trace-id", "x-span-id"}
		foundOtelHeaders := false
		for _, headerKey := range otelHeaders {
			if values, exists := md[headerKey]; exists {
				foundOtelHeaders = true
				for _, value := range values {
					otelInfo += fmt.Sprintf("  %s: %s\n", headerKey, value)
				}
			}
		}
		if !foundOtelHeaders {
			otelInfo += "No OTEL-specific headers found\n"
		}

		// Add all metadata for completeness
		otelInfo += "\n--- All GRPC Metadata ---\n"
		if len(md) > 0 {
			for key, values := range md {
				for _, value := range values {
					otelInfo += fmt.Sprintf("  %s: %s\n", key, value)
				}
			}
		} else {
			otelInfo += "No GRPC metadata found\n"
		}

		otelInfo += "=== End OTEL Data ===\n\n"

		// Write to temp file
		if _, err := tmpFile.WriteString(otelInfo); err != nil {
			logrus.Errorf("Failed to write OTEL data to temp file: %v", err)
		} else {
			logrus.Infof("OTEL trace data logged to temporary file: %s", tmpFile.Name())
		}

		// Also log to console with structured data
		logFields := logrus.Fields{
			"method":    info.FullMethod,
			"temp_file": tmpFile.Name(),
		}

		if spanContext.IsValid() {
			logFields["trace_id"] = spanContext.TraceID().String()
			logFields["span_id"] = spanContext.SpanID().String()
			logFields["trace_flags"] = spanContext.TraceFlags().String()
			logFields["is_remote"] = spanContext.IsRemote()
		}

		if len(baggageMembers) > 0 {
			baggageMap := make(map[string]string)
			for _, member := range baggageMembers {
				baggageMap[member.Key()] = member.Value()
			}
			logFields["baggage"] = baggageMap
		}

		logrus.WithFields(logFields).Info("OTEL trace context captured")
	}

	// Continue with the original handler
	return handler(ctx, req)
}

func parseConfig(fpath string) error {
	logrus.Info("parse config file: ", fpath)
	data, err := os.ReadFile(fpath)
	if err != nil {
		return errors.Wrapf(err, "failed to read plugin config from %s", fpath)
	}
	if err := json.Unmarshal(data, pconfig); err != nil {
		return errors.Wrapf(err, "failed to parse plugin config from %s", string(data))
	}
	logrus.Infof("snapshotter commitID: %s, rwMode: %s, autoRemove: %v, writableLayerType: %s, asyncRemoveSnapshot: %v",
		commitID, pconfig.RwMode, pconfig.AutoRemoveDev, pconfig.WritableLayerType, pconfig.AsyncRemove)
	return nil
}

// TODO: use github.com/urfave/cli/v2
func main() {
	ctx := context.Background()

	// Initialize OpenTelemetry
	shutdown, err := tracing.InitTracer(ctx)
	if err != nil {
		logrus.Errorf("Failed to initialize tracer: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			logrus.Errorf("Failed to shutdown tracer: %v", err)
		}
	}()

	pconfig = overlaybd.DefaultBootConfig()
	fnConfig := defaultConfigPath
	if len(os.Args) == 2 {
		fnConfig = os.Args[1]
	}
	if err := parseConfig(fnConfig); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	if pconfig.LogReportCaller {
		logrus.SetReportCaller(true)
	}

	metrics.Config = pconfig.ExporterConfig
	if pconfig.ExporterConfig.Enable {
		go metrics.Init()
		logrus.Infof("set Prometheus metrics exporter in http://localhost:%d%s", metrics.Config.Port, metrics.Config.UriPrefix)
	}
	contain := func(fsType string) bool {
		for _, fst := range pconfig.TurboFsType {
			if fst == fsType {
				return true
			}
		}
		return false
	}
	if !contain("ext4") {
		pconfig.TurboFsType = append(pconfig.TurboFsType, "ext4")
	}
	if !contain("erofs") {
		pconfig.TurboFsType = append(pconfig.TurboFsType, "erofs")
	}

	if err := setLogLevel(pconfig.LogLevel); err != nil {
		logrus.Errorf("failed to set log level: %v", err)
	} else {
		logrus.Infof("set log level: %s", pconfig.LogLevel)
	}

	sn, err := overlaybd.NewSnapshotter(pconfig)
	if err != nil {
		logrus.Errorf("failed to init overlaybd snapshotter: %v", err)
		os.Exit(1)
	}
	defer sn.Close()

	// Initialize random seed for request ID generation
	rand.Seed(time.Now().UnixNano())

	// Use simplified tracing with request ID interceptor and header logging
	srv := grpc.NewServer(
		tracing.WithServerTracing(),
		grpc.ChainUnaryInterceptor(
			requestIDInterceptor,
			otelLoggingInterceptor,
		),
	)
	snapshotsapi.RegisterSnapshotsServer(srv, tracing.WithTracing(snapshotservice.FromSnapshotter(sn)))

	address := strings.TrimSpace(pconfig.Address)

	if address == "" {
		logrus.Errorf("invalid address path(%s)", address)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(address), 0700); err != nil {
		logrus.Errorf("failed to create directory %v", filepath.Dir(address))
		os.Exit(1)
	}

	// try to remove the socket file to avoid EADDRINUSE
	if err := os.RemoveAll(address); err != nil {
		logrus.Errorf("failed to remove %v", address)
		os.Exit(1)
	}

	l, err := net.Listen("unix", address)
	if err != nil {
		logrus.Errorf("failed to listen on %s: %v", address, err)
		os.Exit(1)
	}

	go func() {
		if err := srv.Serve(l); err != nil {
			logrus.Errorf("failed to server: %v", err)
			os.Exit(1)
		}
	}()
	logrus.Infof("start to serve overlaybd snapshotter on %s", address)

	signals := make(chan os.Signal, 32)
	signal.Notify(signals, unix.SIGTERM, unix.SIGINT, unix.SIGPIPE)

	<-handleSignals(context.TODO(), signals, srv)

	if pconfig.ExporterConfig.Enable {
		metrics.IsAlive.Set(0)
	}
}

func handleSignals(ctx context.Context, signals chan os.Signal, server *grpc.Server) chan struct{} {
	doneCh := make(chan struct{}, 1)

	go func() {
		for {
			s := <-signals
			switch s {
			case unix.SIGUSR1:
				dumpStacks()
			case unix.SIGPIPE:
				continue
			default:
				if server == nil {
					close(doneCh)
					return
				}

				server.GracefulStop()
				close(doneCh)
				return
			}
		}
	}()

	return doneCh
}

func dumpStacks() {
	var (
		buf       []byte
		stackSize int
	)

	bufferLen := 16384
	for stackSize == len(buf) {
		buf = make([]byte, bufferLen)
		stackSize = runtime.Stack(buf, true)
		bufferLen *= 2
	}

	buf = buf[:stackSize]
	logrus.Infof("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
}

func setLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logrus.SetLevel(logLevel)
	return nil
}
