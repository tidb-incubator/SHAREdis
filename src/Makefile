#
# Makefile
#

all: build

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -gcflags "all=-N -l" -mod=vendor -o bin/sharedis cmd/server/*

# vim:ft=make
#
