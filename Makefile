.PHONY: test
test:
	@go test -parallel 4 -v ./v2/...

.PHONY: ci-test
ci-test:
	@go test -parallel 4 -race -coverprofile=coverage.txt -covermode=atomic -v ./v2/...

.PHONY: cover
cover:
	@go test -parallel 4 -race -coverprofile=coverage.txt -covermode=atomic -v ./v2/...
	@go tool cover -func coverage.txt
	@rm coverage.txt
