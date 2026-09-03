APP_NAME = aqi-bench
APP_PATH = .

# 编译路径
BUILD_PATH := ./dist

# 编译时间
BUILD_DATE = $(shell date +'%F %T')

GIT_BRANCH = -
GIT_COMMIT = -
GIT_REVISION = -

ifneq ($(wildcard .git),)
GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT = $(shell git rev-list --count HEAD)
GIT_REVISION = $(shell git rev-parse --short HEAD)
endif

# support包名称
FLAGS_PKG = github.com/wonli/aqi
LDFLAGS = "-X '$(FLAGS_PKG).BuildDate=$(BUILD_DATE)' \
		   -X '$(FLAGS_PKG).Branch=$(GIT_BRANCH)' \
		   -X '$(FLAGS_PKG).CommitVersion=$(GIT_COMMIT)' \
		   -X '$(FLAGS_PKG).Revision=$(GIT_REVISION)' \
		   -s -w"

# 编译参数
GO_FLAGS = -ldflags $(LDFLAGS) -trimpath -tags netgo

# Go编译命令 1-GOOS 2-GOARCH 3-FILE EXT
define go/build
	go mod tidy
	GOOS=$(1) GOARCH=$(2) CGO_ENABLED=0 go build $(GO_FLAGS) -o $(BUILD_PATH)/$(APP_NAME)-$(1)-$(2)-latest$(3) ${APP_PATH}
endef

# Generate binaries
.PHONY: build darwin linux windows

darwin:
	$(call go/build,darwin,arm64)

linux:
	$(call go/build,linux,amd64)

windows:
	$(call go/build,windows,amd64,.exe)
