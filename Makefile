# PROGRAM_NAME is the name of the GIT repository.
# It should match <artifactId> in pom.xml
PROGRAM_NAME := $(shell basename `git rev-parse --show-toplevel`)

# Git variables

GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_REPOSITORY_NAME := $(shell basename `git rev-parse --show-toplevel`)
GIT_SHA := $(shell git log --pretty=format:'%H' -n 1)
GIT_VERSION := $(shell git describe --always --tags --long --dirty | sed -e 's/\-0//' -e 's/\-g.......//')
GIT_VERSION_LONG := $(shell git describe --always --tags --long --dirty)

# Docker variables

DOCKER_IMAGE_NAME := senzing/data-mart-replicator

# Misc.

TARGET ?= target

# -----------------------------------------------------------------------------
# The first "make" target runs as default.
# -----------------------------------------------------------------------------

.PHONY: default
default: help

# -----------------------------------------------------------------------------
# Local development
# -----------------------------------------------------------------------------

.PHONY: install
install:
	mvn install \
		-Dgit.branch=$(GIT_BRANCH) \
		-Dgit.repository.name=$(GIT_REPOSITORY_NAME) \
		-Dgit.sha=$(GIT_SHA) \
		-Dgit.version.long=$(GIT_VERSION_LONG) \
		-Dproject.version=$(GIT_VERSION) \
		-DskipTests=true

.PHONY: package
package:
	mvn package \
		-Dgit.branch=$(GIT_BRANCH) \
		-Dgit.repository.name=$(GIT_REPOSITORY_NAME) \
		-Dgit.sha=$(GIT_SHA) \
		-Dgit.version.long=$(GIT_VERSION_LONG) \
		-Dproject.version=$(GIT_VERSION) \
		-DskipTests=true

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------

.PHONY: help
help:
	@echo "List of make targets:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs
