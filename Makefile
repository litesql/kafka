
.PRONY: builder
builder:
	docker build -f Dockerfile-builder -t builder-kafka .

.PHONY: snapshot
snapshot: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-kafka goreleaser release --clean --snapshot --skip publish

.PHONY: release
release: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-kafka goreleaser release --clean