.PHONY: all build test demo-up demo-down chaos-test bench bench-writeheavy bench-bp-comparison clean proto lint

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
BINARY_NODE=forgekv
BINARY_CTL=forgekvctl
BINARY_BENCH=forgekvbench

# Build directories
BIN_DIR=./bin
CMD_DIR=./cmd

# Proto
PROTO_DIR=./api
PROTO_OUT=./api/forgekv

all: proto build

proto:
	@echo "Generating protobuf code..."
	@mkdir -p $(PROTO_OUT)
	protoc --go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		-I$(PROTO_DIR) $(PROTO_DIR)/forgekv.proto

build: proto
	@echo "Building binaries..."
	@mkdir -p $(BIN_DIR)
	$(GOBUILD) -o $(BIN_DIR)/$(BINARY_NODE) $(CMD_DIR)/forgekv/main.go
	$(GOBUILD) -o $(BIN_DIR)/$(BINARY_CTL) $(CMD_DIR)/forgekvctl/main.go
	$(GOBUILD) -o $(BIN_DIR)/$(BINARY_BENCH) $(CMD_DIR)/forgekvbench/main.go

test:
	@echo "Running unit tests..."
	$(GOTEST) -v -race -cover ./internal/...

test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v -race -tags=integration ./tests/...

demo-up:
	@echo "Starting ForgeKV demo cluster..."
	@mkdir -p /tmp/forgekv/n1 /tmp/forgekv/n2 /tmp/forgekv/n3
	@echo "Starting observability stack..."
	docker-compose -f deploy/docker-compose.yaml up -d
	@sleep 3
	@echo "Starting ForgeKV nodes..."
	$(BIN_DIR)/$(BINARY_NODE) node --id n1 --data-dir /tmp/forgekv/n1 \
		--client-addr 127.0.0.1:9001 --raft-addr 127.0.0.1:9101 --admin-addr 127.0.0.1:9201 --metrics-addr 127.0.0.1:9301 \
		--peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103 \
		--peer-client-addrs n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 &
	$(BIN_DIR)/$(BINARY_NODE) node --id n2 --data-dir /tmp/forgekv/n2 \
		--client-addr 127.0.0.1:9002 --raft-addr 127.0.0.1:9102 --admin-addr 127.0.0.1:9202 --metrics-addr 127.0.0.1:9302 \
		--peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103 \
		--peer-client-addrs n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 &
	$(BIN_DIR)/$(BINARY_NODE) node --id n3 --data-dir /tmp/forgekv/n3 \
		--client-addr 127.0.0.1:9003 --raft-addr 127.0.0.1:9103 --admin-addr 127.0.0.1:9203 --metrics-addr 127.0.0.1:9303 \
		--peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103 \
		--peer-client-addrs n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 &
	@sleep 3
	@echo "ForgeKV cluster is up!"
	@echo "  Node 1: client=127.0.0.1:9001 raft=127.0.0.1:9101 admin=127.0.0.1:9201"
	@echo "  Node 2: client=127.0.0.1:9002 raft=127.0.0.1:9102 admin=127.0.0.1:9202"
	@echo "  Node 3: client=127.0.0.1:9003 raft=127.0.0.1:9103 admin=127.0.0.1:9203"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  Jaeger: http://localhost:16686"

demo-down:
	@echo "Stopping ForgeKV demo cluster..."
	-pkill -f "$(BINARY_NODE) node" || true
	docker-compose -f deploy/docker-compose.yaml down
	rm -rf /tmp/forgekv
	@echo "ForgeKV cluster stopped."

chaos-test:
	@echo "Running chaos tests with linearizability checks..."
	$(GOTEST) -v -race -tags=chaos -timeout 5m ./tests/chaos/...

bench:
	@echo "Running benchmarks..."
	@mkdir -p bench
	$(BIN_DIR)/$(BINARY_BENCH) --workload mixed --duration 60s --concurrency 64 --output bench/REPORT.md
	@echo "Benchmark report written to bench/REPORT.md"

bench-writeheavy:
	@echo "Running write-heavy benchmarks..."
	@mkdir -p bench
	$(BIN_DIR)/$(BINARY_BENCH) --workload writeheavy --duration 60s --concurrency 64 --measure-failover=false --output bench/REPORT-writeheavy.md
	@echo "Benchmark report written to bench/REPORT-writeheavy.md"

bench-bp-comparison: build
	@echo "Running backpressure A/B comparison..."
	./scripts/bench-bp-comparison.sh

lint:
	@echo "Running linters..."
	golangci-lint run ./...

clean:
	@echo "Cleaning up..."
	rm -rf $(BIN_DIR)
	rm -rf $(PROTO_OUT)/*.pb.go
	rm -rf /tmp/forgekv

deps:
	@echo "Installing dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Installing protoc plugins..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
