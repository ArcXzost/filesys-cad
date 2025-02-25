# Build the binary
build:
	@echo "Building the application..."
	@go build -o bin/fs

# Run the application with CLI commands
run: build
	@echo "Running the application..."
	@./bin/fs

# Run tests
test:
	@echo "Running tests..."
	@go test ./...

# Clean up build artifacts
clean:
	@echo "Cleaning up..."
	@rm -rf bin/