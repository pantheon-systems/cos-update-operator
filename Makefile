.PHONY:	all image clean test coverage lint image operator-image agent-image push proto
VERSION := $(shell ./build/git-version.sh)
RELEASE_VERSION := $(shell cat VERSION)
COMMIT := $(shell git rev-parse HEAD)

ifneq ($(VERSION), $(RELEASE_VERSION))
    VERSION := $(RELEASE_VERSION)-$(VERSION)
endif

ifneq ($(CIRCLE_BRANCH),)
    BRANCH_LAST_PART := $(lastword $(subst /, ,$(CIRCLE_BRANCH)))
    VERSION := $(CIRCLE_BUILD_NUM)-$(BRANCH_LAST_PART)
endif

REPO=github.com/pantheon-systems/cos-update-operator
# ko can't pass -ldflags any other way
GOFLAGS := -ldflags=-w
GOFLAGS := $(GOFLAGS) -ldflags=-X=$(REPO)/pkg/version.Version=$(RELEASE_VERSION)
GOFLAGS := "$(GOFLAGS) -ldflags=-X=$(REPO)/pkg/version.Commit=$(COMMIT)"

OPERATOR_IMAGE_REPO ?= us-docker.pkg.dev/pantheon-artifacts/internal/cos-update-operator
AGENT_IMAGE_REPO ?= us-docker.pkg.dev/pantheon-artifacts/internal/cos-update-operator-agent

KUBE_NAMESPACE ?= $(shell kubectl config get-contexts \
    | grep $(kubectl config current-context) | awk '{ print $NF}')

# set GOGC to mitigate OOMing and set lint cache location for use with circleci cache
ifdef CIRCLECI
    export GOLANGCI_LINT_CACHE=/tmp/golangci-lint-cache
    export GOGC=50
endif

all: bin/* test lint coverage

# used for caching golangci-lint data in circleci
master-sha:
	git fetch origin && git rev-parse origin/master > master_sha

test: deps
bin/*: deps

tools:
	@GOBIN=$$(go env GOPATH)/bin; \
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$GOBIN v1.62.2
	go install "github.com/ory/go-acc@latest"

deps: tools proto
	go get ./...

export CGO_ENABLED := 0

# support locally executable operator binary for use with `kubectl proxy`
GOOS ?= $(shell go env GOOS)
bin/update-operator: test
	GOFLAGS=$(GOFLAGS) GOARCH=amd64 GOOS=$(GOOS) go build -o bin/update-operator \
        $(REPO)/cmd/update-operator

# default to linux because this binary is meant to only run on a linux host
GOOS ?= linux
bin/update-agent: test
	GOFLAGS=$(GOFLAGS) GOARCH=amd64 GOOS=$(GOOS) go build -o bin/update-agent \
        $(REPO)/cmd/update-agent

test:
	go test -v $(REPO)/pkg/...

lint:
	golangci-lint run --verbose

coverage:
	go-acc ./...

agent-image: bin/update-agent
	docker build -t $(AGENT_IMAGE_REPO):$(VERSION) --build-arg=cmd=update-agent .

operator-image: bin/update-operator
	docker build -t $(OPERATOR_IMAGE_REPO):$(VERSION) --build-arg=cmd=update-operator .

image: agent-image
image: operator-image

push-agent: agent-image
	docker push $(AGENT_IMAGE_REPO):$(VERSION)
ifeq ($(CIRCLE_BRANCH),master)
	docker tag $(AGENT_IMAGE_REPO):$(VERSION) $(AGENT_IMAGE_REPO):master
	docker push $(AGENT_IMAGE_REPO):master
endif

push-operator: operator-image
	docker push $(OPERATOR_IMAGE_REPO):$(VERSION)
ifeq ($(CIRCLE_BRANCH),master)
	docker tag $(OPERATOR_IMAGE_REPO):$(VERSION) $(OPERATOR_IMAGE_REPO):master
	docker push $(OPERATOR_IMAGE_REPO):master
endif

push: push-agent push-operator

ko: export KO_DOCKER_REPO=us-central1-docker.pkg.dev/pantheon-sandbox/pantheon-sandbox-public
ko:
	ko apply -f k8s/daemonset.yaml

# requires protoc with go support
# for macOS install with: brew install protoc-gen-go
proto:
	curl --silent https://chromium.googlesource.com/chromiumos/platform2/+/master/system_api/dbus/update_engine/update_engine.proto?format=TEXT \
		| base64 --decode \
		| awk '1;/syntax/{ print "option go_package = \"pkg/updateengine\";"; }' \
		> pkg/updateengine/update_engine.proto
	protoc --go_out=. --go_opt=paths=source_relative pkg/updateengine/update_engine.proto

clean:
	rm -rf bin
	rm pkg/updateengine/update_engine.pb.go
	rm pkg/updateengine/update_engine.proto
