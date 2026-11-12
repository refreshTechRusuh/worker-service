run:
	go run cmd/main.go
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/worker cmd/main.go
test:
	go test -v ./...
mod-tidy:
	go mod tidy

scan:
	@echo "Running security scan"
	gosec ./...
vuln:
	@echo "Running vulnerability scan"
	govulncheck ./...
