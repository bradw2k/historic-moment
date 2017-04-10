#!/bin/bash
GOOS=linux GOARCH=amd64 go build -o historic_moment_linux
GOOS=darwin GOARCH=amd64 go build -o historic_moment
